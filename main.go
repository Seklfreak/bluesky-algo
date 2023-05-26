package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/events"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/repomgr"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"nhooyr.io/websocket"
)

func main() {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		panic(fmt.Errorf("error creating logger: %w", err))
	}

	dbX, err := sqlx.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal("error opening database", zap.Error(err))
	}
	defer dbX.Close()
	err = dbX.PingContext(ctx)
	if err != nil {
		log.Fatal("error pinging database", zap.Error(err))
	}

	_, err = dbX.ExecContext(ctx, migrations)
	if err != nil {
		log.Fatal("error running migrations", zap.Error(err))
	}

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/xrpc/app.bsky.feed.getFeedSkeleton", func(w http.ResponseWriter, r *http.Request) {
		var output bsky.FeedGetFeedSkeleton_Output

		var post string
		err := dbX.GetContext(
			ctx,
			&post,
			`
SELECT uri
FROM posts
ORDER BY indexedAt DESC
LIMIT 1
;
`,
		)
		if err != nil {
			log.Error("error getting latest post", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		output.Feed = []*bsky.FeedDefs_SkeletonFeedPost{
			{
				Post: post,
			},
		}

		json.NewEncoder(w).Encode(output)
	})
	go func() {
		http.ListenAndServe(":4000", r)
	}()

	wssConn, _, err := websocket.Dial(ctx, "wss://bsky.social/xrpc/com.atproto.sync.subscribeRepos", nil)
	if err != nil {
		log.Fatal("error dialing bluesky websocket", zap.Error(err))
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
		log.Fatal("error creating session", zap.Error(err))
	}

	xrpcClient := newXRPCClient(&xrpc.AuthInfo{
		AccessJwt:  xrpcSession.AccessJwt,
		RefreshJwt: xrpcSession.RefreshJwt,
		Handle:     xrpcSession.Handle,
		Did:        xrpcSession.Did,
	})
	_ = xrpcClient

	//go func() {
	//	ticker := time.NewTicker(30 * time.Second)
	//	defer ticker.Stop()
	//
	//	var err error
	//	for {
	//		select {
	//		case <-ticker.C:
	//			err = wssConn.Ping(ctx)
	//			if err != nil {
	//				log.Warn("error pinging websocket", zap.Error(err))
	//			}
	//		case <-ctx.Done():
	//			return
	//		}
	//	}
	//}()

	var reader io.Reader
	for {
		_, reader, err = wssConn.Reader(ctx)
		if err != nil {
			log.Fatal("error getting reader from websocket connection", zap.Error(err))
		}

		var header events.EventHeader
		err = header.UnmarshalCBOR(reader)
		if err != nil {
			log.Fatal("error reading event header", zap.Error(err))
		}

		switch header.Op {
		case events.EvtKindMessage:

			switch header.MsgType {
			case "#commit":
				var commitEvent atproto.SyncSubscribeRepos_Commit
				err := commitEvent.UnmarshalCBOR(reader)
				if err != nil {
					log.Fatal("error reading commit event", zap.Error(err))
				}

				// TODO: check for out of order events? see events.HandleRepoStream()

				eventRepo, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(commitEvent.Blocks))
				if err != nil {
					log.Fatal("error reading repo from car", zap.Error(err))
				}

				for _, opt := range commitEvent.Ops {
					switch repomgr.EventKind(opt.Action) {
					// TODO: implement update, delete
					case repomgr.EvtKindCreateRecord:
						_, record, err := eventRepo.GetRecord(ctx, opt.Path)
						if err != nil {
							log.Fatal("error getting record from repo", zap.Error(err))
						}

						switch r := record.(type) {
						case *bsky.FeedPost:
							// TODO: cache profiles
							//profile, err := bsky.ActorGetProfile(ctx, xrpcClient, commitEvent.Repo)
							//if err != nil {
							//	panic(fmt.Errorf("error getting profile: %w", err))
							//}

							// fmt.Printf("new post: %q by %s\n", r.Text, profile.Handle)
							log.Debug("new post", zap.String("text", r.Text))

							_, err := dbX.ExecContext(
								ctx,
								`
INSERT INTO posts (uri, cid, replyParent, replyRoot, indexedAt, text, createdAt)
VALUES ($1, $2, $3, $4, $5, $6, $7)
;
`,
								opt.Path,
								opt.Cid.String(),
								func() *string {
									if r.Reply == nil || r.Reply.Parent == nil {
										return nil
									}

									uri := r.Reply.Parent.Uri
									return &uri
								}(),
								func() *string {
									if r.Reply == nil || r.Reply.Root == nil {
										return nil
									}

									uri := r.Reply.Root.Uri
									return &uri
								}(),
								time.Now(),
								r.Text,
								r.CreatedAt, // TODO: fix date parsing
							)
							if err != nil {
								log.Fatal("error inserting post into database", zap.Error(err))
							}
						}
					}
				}
			}

		case events.EvtKindErrorFrame:
			var errframe events.ErrorFrame
			err := errframe.UnmarshalCBOR(reader)
			if err != nil {
				log.Fatal("error reading error frame", zap.Error(err))
			}

			log.Fatal("received error frame", zap.String("error", errframe.Error), zap.String("message", errframe.Message))
		default:
			log.Fatal("received unrecognized event stream type", zap.Int64("type", header.Op))
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
