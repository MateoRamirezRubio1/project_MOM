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

func buildServer() *gin.Engine {
	/* ───── flags ───── */
	dataDir := flag.String("data", "./data", "Badger dir")
	httpAddr := flag.String("http", ":8080", "REST bind")
	clusterCF := flag.String("cluster", "cluster.json", "cluster config")
	flag.Parse()

	/* ───── Badger ───── */
	store, err := badgerstore.New(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	cluster.RebuildHWM(store.DB()) // mantiene compatibilidad
	store.StartRequeueLoop()
	catalog := badgermeta.New(store.DB())

	/* ───── auth demo ───── */
	authStore := authadapter.NewInMemory()

	/* ───── cluster (opcional) ───── */
	var fan *cluster.Fanout
	selfID := os.Getenv("NODE_ID")
	cfg, _ := cluster.Load(*clusterCF)
	cluster.GlobalCfg, cluster.GlobalSelfID = cfg, selfID // ★

	if cfg != nil && selfID != "" {
		fan = cluster.NewFanout(cfg, selfID)
		if n := cfg.Self(selfID); n != nil {
			// ← se pasa también catalog
			cluster.StartGRPCServer(n.Host, store, catalog)
			log.Printf("[cluster] node %s activo (%s)", selfID, n.Host)
		}
	}

	/* ───── use-cases ───── */
	adminUC := usecase.NewAdmin(catalog)
	pubUC := usecase.NewPublisher(catalog, store, authStore, fan)
	consUC := usecase.NewConsumer(catalog, store)
	queueUC := usecase.NewQueue(catalog, store)

	/* ───── router ───── */
	r := restadapter.NewRouter(adminUC, pubUC, consUC, queueUC, authStore)
	go func() {
		log.Printf("[REST] escuchando en %s", *httpAddr)
		if err := r.Run(*httpAddr); err != nil {
			log.Fatal(err)
		}
	}()
	return r
}
