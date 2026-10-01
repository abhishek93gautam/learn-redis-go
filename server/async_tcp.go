package server

import (
	"log"
	"net"
	"syscall"
	"time"

	"learn/redis/config"
	"learn/redis/core"
)

var con_clients int = 0
var cronFrequency time.Duration = 1 * time.Second
var lastCronExecTime time.Time = time.Now()

func RunAsyncTCPServer() error {
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

	for {

		if time.Now().After(lastCronExecTime.Add(cronFrequency)) {
			core.DeleteExpiredKeys()
			lastCronExecTime = time.Now()
		}

		// see if any FD is ready for an IO
		// nevents, e := syscall.EpollWait(epollFD, events[:], -1)

		// Block until at least one FD is ready for IO.
		// Passing nil timeout blocks indefinitely, like EpollWait(..., -1).
		nevents, e := syscall.Kevent(kqFD, nil, events, nil)

		if e != nil {
			continue
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
				con_clients++
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
					con_clients--
					continue
				}
			} else {
				// comm := core.FDComm{Fd: int(events[i].Fd)}
				comm := core.FDComm{Fd: fd}
				cmds, err := readCommands(comm)
				if err != nil {
					// syscall.Close(int(events[i].Fd))
					syscall.Close(fd)
					con_clients -= 1
					continue
				}
				respond(cmds, comm)
			}
		}
	}
}
