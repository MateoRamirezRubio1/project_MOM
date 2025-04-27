package main

import (
	"log"

	"github.com/gin-gonic/gin"

	// adapters
	authadapter "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/auth"
	badgermeta "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/meta/badger"
	restadapter "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/rest"
	badgerstore "github.com/MateoRamirezRubio1/project_MOM/internal/adapters/storage/badger"

	// use-cases
	"github.com/MateoRamirezRubio1/project_MOM/internal/app/usecase"
)

func buildServer() *gin.Engine {
	//------------------------------------------------------------------
	// Infraestructura de persistencia (BadgerDB)
	//------------------------------------------------------------------
	store, err := badgerstore.New("./data")
	if err != nil {
		log.Fatalf("opening badger store: %v", err)
	}
	store.StartRequeueLoop()

	catalog := badgermeta.New(store.DB()) // meta-store comparte la misma BD

	//------------------------------------------------------------------
	// Autenticación en memoria
	//------------------------------------------------------------------
	authStore := authadapter.NewInMemory()

	//------------------------------------------------------------------
	// Casos de uso (hexagonal)
	//------------------------------------------------------------------
	adminUC := usecase.NewAdmin(catalog)                     // <- solo MetaStore
	pubUC := usecase.NewPublisher(catalog, store, authStore) // publicación
	consUC := usecase.NewConsumer(catalog, store)            // consumo
	queueUC := usecase.NewQueue(catalog, store)              // <- Meta+Store (sin auth)

	//------------------------------------------------------------------
	// Router HTTP
	//------------------------------------------------------------------
	return restadapter.NewRouter(adminUC, pubUC, consUC, queueUC, authStore)
}
