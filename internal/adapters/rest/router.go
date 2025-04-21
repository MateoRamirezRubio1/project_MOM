package rest

import (
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/gin-gonic/gin"
)

func NewRouter(admin inbound.Admin, pub inbound.Publisher, cons inbound.Consumer,
	queue inbound.Queue, auth outbound.AuthStore) *gin.Engine {

	r := gin.Default()
	h := NewHandlers(admin, pub, cons, queue)

	r.POST("/login", h.Login)

	authMw := AuthMiddleware(auth)

	// tópicos
	r.POST("/topics", authMw, h.CreateTopic)
	r.GET("/topics", authMw, h.ListTopics)
	r.POST("/topics/:topic/messages", authMw, h.Publish)
	r.GET("/topics/:topic/messages", authMw, h.Pull)
	r.POST("/topics/:topic/offsets", authMw, h.CommitOffset)

	// colas
	r.POST("/queues", authMw, h.CreateQueue)
	r.POST("/queues/:queue/messages", authMw, h.Enqueue)
	r.GET("/queues/:queue/messages", authMw, h.Dequeue)
	r.POST("/queues/:queue/ack", authMw, h.Ack)

	return r
}
