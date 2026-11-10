package server

import (
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"learn/redis/config"
	"learn/redis/core"
)

var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()

const EngineStatus_WAITING int32 = 1 << 1
const EngineStatus_BUSY int32 = 1 << 2
const EngineStatus_SHUTTING_DOWN int32 = 1 << 3
const EngineStatus_TRANSACTION int32 = 1 << 4

var eStatus int32 = EngineStatus_WAITING

var connectedClients map[int]*core.Client

func init() {
	connectedClients = make(map[int]*core.Client)
}

func WaitForSignal(wg *sync.WaitGroup, sigs chan os.Signal) {
	defer wg.Done()
	<-sigs

	// if server is busy continue to wait
	for atomic.LoadInt32(&eStatus) == EngineStatus_BUSY {
	}

	// CRITICAL TO HANDLE
	// We do not want server to ever go back to BUSY state
	// when control flow is here ->

	// immediately set the status to be SHUTTING DOWN
	// the only place where we can set the status to be SHUTTING DOWN
	atomic.StoreInt32(&eStatus, EngineStatus_SHUTTING_DOWN)

	// if server is in any other state, initiate a shutdown
	core.Shutdown()
	os.Exit(0)
}

func RunAsyncTCPServer(wg *sync.WaitGroup) error {
	defer wg.Done()
	defer func() {
		atomic.StoreInt32(&eStatus, EngineStatus_SHUTTING_DOWN)
	}()

	log.Println("starting an asynchronous TCP server on", config.Host, config.Port)

	max_clients := 20000

	// Create EPOLL Event Objects to hold events
	// var events []syscall.EpollEvent = make([]syscall.EpollEvent, max_clients)

	// Create a socket
	serverFD, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		return err
	}
	defer syscall.Close(serverFD)

	// Set the Socket operate in a non-blocking mode
	if err = syscall.SetNonblock(serverFD, true); err != nil {
		return err
	}

	// Allow quick restarts (optional, but usually wanted)
	if err = syscall.SetsockoptInt(serverFD, syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1); err != nil {
		return err
	}

	// Bind the IP and the port
	ip4 := net.ParseIP(config.Host)
	if err = syscall.Bind(serverFD, &syscall.SockaddrInet4{
		Port: config.Port,
		Addr: [4]byte{ip4[0], ip4[1], ip4[2], ip4[3]},
	}); err != nil {
		return err
	}

	// Start listening
	if err = syscall.Listen(serverFD, max_clients); err != nil {
		return err
	}

	// AsyncIO starts here!! (kqueue instead of epoll)

	// creating EPOLL instance
	// epollFD, err := syscall.EpollCreate1(0)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer syscall.Close(epollFD)
	kqFD, err := syscall.Kqueue()
	if err != nil {
		return err
	}
	defer syscall.Close(kqFD)

	// Specify the events we want to get hints about
	// and set the socket on which
	// var socketServerEvent syscall.EpollEvent = syscall.EpollEvent{
	// 	Events: syscall.EPOLLIN,
	// 	Fd:     int32(serverFD),
	// }

	// // Listen to read events on the Server itself
	// if err = syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, serverFD, &socketServerEvent); err != nil {
	// 	return err
	// }
	changes := []syscall.Kevent_t{
		{
			Ident:  uint64(serverFD),
			Filter: syscall.EVFILT_READ,
			Flags:  syscall.EV_ADD,
		},
	}
	if _, err = syscall.Kevent(kqFD, changes, nil, nil); err != nil {
		return err
	}

	// Buffer to receive fired events into (analogue of the []EpollEvent slice)
	events := make([]syscall.Kevent_t, max_clients)

	for atomic.LoadInt32(&eStatus) != EngineStatus_SHUTTING_DOWN {

		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		// Say, the Engine triggered SHUTTING down when the control flow is here ->
		// Current: Engine status == WAITING
		// Update: Engine status = SHUTTING_DOWN
		// Then we have to exit (handled in Signal Handler)

		// see if any FD is ready for an IO
		// nevents, e := syscall.EpollWait(epollFD, events[:], -1)

		// Block until at least one FD is ready for IO.
		// Passing nil timeout blocks indefinitely, like EpollWait(..., -1).
		nevents, e := syscall.Kevent(kqFD, nil, events, nil)

		if e != nil {
			continue
		}

		// Here, we do not want server to go back from SHUTTING DOWN
		// to BUSY
		// If the engine status == SHUTTING_DOWN over here ->
		// We have to exit
		// hence the only legal transitiion is from WAITING to BUSY
		// if that does not happen then we can exit.

		// mark engine as BUSY only when it is in the waiting state
		if !atomic.CompareAndSwapInt32(&eStatus, EngineStatus_WAITING, EngineStatus_BUSY) {
			// if swap unsuccessful then the existing status is not WAITING, but something else
			switch eStatus {
			case EngineStatus_SHUTTING_DOWN:
				return nil
			}
		}

		for i := 0; i < nevents; i++ {
			// if the socket server itself is ready for an IO
			fd := int(events[i].Ident)

			if fd == serverFD {
				// accept the incoming connection from a client
				connFD, _, err := syscall.Accept(serverFD)
				if err != nil {
					log.Println("err", err)
					continue
				}

				// increase the number of concurrent clients count
				connectedClients[connFD] = core.NewClient(connFD)
				syscall.SetNonblock(serverFD, true)

				// add this new TCP connection to be monitored
				// var socketClientEvent syscall.EpollEvent = syscall.EpollEvent{
				// 	Events: syscall.EPOLLIN,
				// 	Fd:     int32(fd),
				// }
				// if err := syscall.EpollCtl(epollFD, syscall.EPOLL_CTL_ADD, fd, &socketClientEvent); err != nil {
				// 	log.Fatal(err)
				// }

				clientChange := []syscall.Kevent_t{
					{
						Ident:  uint64(connFD),
						Filter: syscall.EVFILT_READ,
						Flags:  syscall.EV_ADD,
					},
				}
				if _, err := syscall.Kevent(kqFD, clientChange, nil, nil); err != nil {
					log.Println("err", err)
					syscall.Close(connFD)
					// delete(connectedClients, connFD)
					continue
				}
			} else {
				// comm := core.FDComm{Fd: int(events[i].Fd)}
				// comm := core.FDComm{Fd: fd}
				comm := connectedClients[int(events[i].Ident)]
				if comm == nil {
					continue
				}
				cmds, err := readCommands(comm)
				if err != nil {
					// syscall.Close(int(events[i].Fd))
					syscall.Close(fd)
					delete(connectedClients, int(events[i].Ident))
					continue
				}
				respond(cmds, comm)
			}
		}
		// mark engine as WAITING
		// no contention as the signal handler is blocked until
		// the engine is BUSY
		atomic.StoreInt32(&eStatus, EngineStatus_WAITING)
	}
	return nil
}
