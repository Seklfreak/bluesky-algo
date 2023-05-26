package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"nhooyr.io/websocket"
)

func main() {
	ctx := context.Background()

	dbX, err := sqlx.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		panic(fmt.Errorf("error opening database: %w", err))
	}
	defer dbX.Close()
	err = dbX.PingContext(ctx)
	if err != nil {
		panic(fmt.Errorf("error pinging database: %w", err))
	}

	wssConn, _, err := websocket.Dial(ctx, "wss://bsky.social/xrpc/com.atproto.sync.subscribeRepos", nil)
	if err != nil {
		panic(fmt.Errorf("error dialing bluesky websocket: %w", err))
	}
	defer wssConn.Close(websocket.StatusInternalError, "unexpected shutdown")

	xrpcSession, err := atproto.ServerCreateSession(
		ctx,
		newXRPCClient(nil),
		&atproto.ServerCreateSession_Input{
			Identifier: os.Getenv("BSKY_USERNAME"),
			Password:   os.Getenv("BSKY_PASSWORD"),
		},
	)
	if err != nil {
		panic(fmt.Errorf("error creating session: %w", err))
	}

	xrpcClient := newXRPCClient(&xrpc.AuthInfo{
		AccessJwt:  xrpcSession.AccessJwt,
		RefreshJwt: xrpcSession.RefreshJwt,
		Handle:     xrpcSession.Handle,
		Did:        xrpcSession.Did,
	})
	_ = xrpcClient

	// TODO: ping frequently? see events.HandleRepoStream()

	var reader io.Reader
	for {
		_, reader, err = wssConn.Reader(ctx)
		if err != nil {
			panic(fmt.Errorf("error getting reader from websocket connection: %w", err))
		}

		var header events.EventHeader
		err = header.UnmarshalCBOR(reader)
		if err != nil {
			panic(fmt.Errorf("error reading event header: %w", err))
		}

		switch header.Op {
		case events.EvtKindMessage:

			switch header.MsgType {
			case "#commit":
				var commitEvent atproto.SyncSubscribeRepos_Commit
				err := commitEvent.UnmarshalCBOR(reader)
				if err != nil {
					panic(fmt.Errorf("error reading commit event: %w", err))
				}

				// TODO: check for out of order events? see events.HandleRepoStream()

				eventRepo, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(commitEvent.Blocks))
				if err != nil {
					panic(fmt.Errorf("error reading repo from car: %w", err))
				}

				for _, opt := range commitEvent.Ops {
					switch repomgr.EventKind(opt.Action) {
					case repomgr.EvtKindCreateRecord:
						_, record, err := eventRepo.GetRecord(ctx, opt.Path)
						if err != nil {
							panic(fmt.Errorf("error getting record from repo: %w", err))
						}

						switch r := record.(type) {
						case *bsky.FeedPost:
							// TODO: cache profiles
							//profile, err := bsky.ActorGetProfile(ctx, xrpcClient, commitEvent.Repo)
							//if err != nil {
							//	panic(fmt.Errorf("error getting profile: %w", err))
							//}

							// fmt.Printf("new post: %q by %s\n", r.Text, profile.Handle)
							fmt.Printf("new post: %q\n", r.Text)
						}
					}
				}
			}

		case events.EvtKindErrorFrame:
			var errframe events.ErrorFrame
			err := errframe.UnmarshalCBOR(reader)
			if err != nil {
				panic(fmt.Errorf("error reading error frame: %w", err))
			}

			panic(fmt.Errorf("received error frame: %s: %s", errframe.Error, errframe.Message))
		default:
			panic(fmt.Errorf("received unrecognized event stream type: %d", header.Op))
		}
	}

	// TODO: add graceful shutdown
	// err = c.Close(websocket.StatusNormalClosure, "")
	// if err != nil {
	// 	panic(err)
	// }
}

func newXRPCClient(auth *xrpc.AuthInfo) *xrpc.Client {
	return &xrpc.Client{
		Host: "https://bsky.social",
		UserAgent: func() *string {
			val := "github.com/Seklfreak/bluesky-algo"
			return &val
		}(),
		Auth: auth,
	}
}
