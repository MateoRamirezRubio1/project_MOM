package main

import (
	"log"
	"os"

	"github.com/MateoRamirezRubio1/project_MOM/internal/adapters/auth"
	badgermeta "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/meta/badger"
	"github.com/MateoRamirezRubio1/project_MOM/internal/adapters/rest"
	badgerstore "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/storage/badger"
	"github.com/MateoRamirezRubio1/project_MOM/internal/app/usecase"
	"github.com/gin-gonic/gin"
)

func BuildServer() *gin.Engine {
	// ---------- Badger storage path -------------
	dataDir := "./data"
	if v := os.Getenv("MOM_DATA"); v != "" {
		dataDir = v
	}

	// ---------- Message store (persistent) ------
	msgStore, err := badgerstore.New(dataDir)
	if err != nil {
		log.Fatalf("badger msgStore: %v", err)
	}
	msgStore.StartRequeueLoop()

	// ---------- Meta store persistent -----------
	metaStore := badgermeta.New(msgStore.DB())

	// ---------- Auth store -----------------------
	authStore := auth.NewInMemory()

	// ---------- Use‑cases ------------------------
	adminUC := usecase.NewAdmin(metaStore, authStore)
	pubUC := usecase.NewPublisher(metaStore, msgStore, authStore)
	conUC := usecase.NewConsumer(metaStore, msgStore)
	queueUC := usecase.NewQueue(metaStore, msgStore, authStore)

	return rest.NewRouter(adminUC, pubUC, conUC, queueUC, authStore)
}
