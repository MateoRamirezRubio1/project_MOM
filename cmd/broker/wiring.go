package main

import (
	"flag"
	"log"
	"os"

	"github.com/gin-gonic/gin"

	// adapters
	authadapter "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/auth"
	badgermeta "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/meta/badger"
	restadapter "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/rest"
	badgerstore "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/storage/badger"
	"github.com/MateoRamirezRubio1/project_MOM/internal/cluster"

	// use-cases
	"github.com/MateoRamirezRubio1/project_MOM/internal/app/usecase"
)

// buildServer returns the HTTP router and – IF this process belongs to the
// cluster – also starts a gRPC replica-endpoint in a goroutine (fire-and-forget)
func buildServer() *gin.Engine {
	//----------------------------------------------------------------------
	// CLI flags -----------------------------------------------------------
	//----------------------------------------------------------------------
	dataDir := flag.String("data", "./data", "badger dir (per-node)")
	httpAddr := flag.String("http", ":8080", "REST listen addr")
	clusterCF := flag.String("cluster", "cluster.json", "cluster config file")
	flag.Parse()

	//----------------------------------------------------------------------
	// Persistent storage (Badger) ----------------------------------------
	//----------------------------------------------------------------------
	store, err := badgerstore.New(*dataDir)
	if err != nil {
		log.Fatalf("badger: %v", err)
	}
	store.StartRequeueLoop()
	catalog := badgermeta.New(store.DB())

	//----------------------------------------------------------------------
	// In-memory auth (demo) ----------------------------------------------
	//----------------------------------------------------------------------
	authStore := authadapter.NewInMemory()

	//----------------------------------------------------------------------
	// Cluster set-up (optional) ------------------------------------------
	//----------------------------------------------------------------------
	var fan *cluster.Fanout
	selfID := os.Getenv("NODE_ID") // "n1", "n2", …
	if cfg, err := cluster.Load(*clusterCF); err == nil && selfID != "" {
		fan = cluster.NewFanout(cfg, selfID) // nil if single-node
		if n := cfg.Self(selfID); n != nil { // run replica listener
			cluster.StartGRPCServer(n.Host, store)
			log.Printf("[cluster] node %s up (%s)", selfID, n.Host)
		}
	}

	//----------------------------------------------------------------------
	// Hex-use-cases -------------------------------------------------------
	//----------------------------------------------------------------------
	adminUC := usecase.NewAdmin(catalog)
	pubUC := usecase.NewPublisher(catalog, store, authStore, fan)
	consUC := usecase.NewConsumer(catalog, store)
	queueUC := usecase.NewQueue(catalog, store)

	//----------------------------------------------------------------------
	// Gin router ----------------------------------------------------------
	//----------------------------------------------------------------------
	r := restadapter.NewRouter(adminUC, pubUC, consUC, queueUC, authStore)
	go func() {
		log.Printf("[REST] listening on %s", *httpAddr)
		if err := r.Run(*httpAddr); err != nil {
			log.Fatalf("[REST] %v", err)
		}
	}()
	return r
}
