package rest

import (
	"net/http"
	"strconv"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/inbound"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type Handlers struct {
	admin    inbound.Admin
	pub      inbound.Publisher
	consumer inbound.Consumer
	queue    inbound.Queue
}

func NewHandlers(a inbound.Admin, p inbound.Publisher, c inbound.Consumer, q inbound.Queue) *Handlers {
	return &Handlers{admin: a, pub: p, consumer: c, queue: q}
}

// ---- AUTH -------------------------------------------------------

func (h *Handlers) Login(c *gin.Context) {
	var u struct {
		User string `json:"user"`
		Pass string `json:"pass"`
	}
	_ = c.BindJSON(&u)
	c.JSON(http.StatusOK, gin.H{"token": u.User})
}

// ---- TOPICS -----------------------------------------------------

func (h *Handlers) CreateTopic(c *gin.Context) { /* igual que antes */ }

func (h *Handlers) ListTopics(c *gin.Context) { /* igual */ }

func (h *Handlers) Publish(c *gin.Context) { /* igual */ }

func (h *Handlers) Pull(c *gin.Context) {
	topic := c.Param("topic")
	group := c.DefaultQuery("group", "default")
	part, _ := strconv.Atoi(c.DefaultQuery("partition", "0"))
	max, _ := strconv.Atoi(c.DefaultQuery("max", "100"))

	msgs, err := h.consumer.Pull(c, topic, group, part, max)
	if err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, msgs)
}

func (h *Handlers) CommitOffset(c *gin.Context) {
	topic := c.Param("topic")
	var req struct {
		Group     string `json:"group"`
		Partition int    `json:"partition"`
		Offset    uint64 `json:"offset"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := h.consumer.Commit(c, topic, req.Group, req.Partition, req.Offset); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.Status(204)
}

// ---- QUEUES -----------------------------------------------------

func (h *Handlers) CreateQueue(c *gin.Context) {
	var req struct {
		Name string `json:"name"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	user := c.GetString("user")
	if err := h.queue.CreateQueue(c, req.Name, user); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.Status(201)
}

func (h *Handlers) Enqueue(c *gin.Context) {
	queue := c.Param("queue")
	var req struct {
		Payload string `json:"payload"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	user := c.GetString("user")
	if err := h.queue.Enqueue(c, queue, req.Payload, user); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.Status(201)
}

func (h *Handlers) Dequeue(c *gin.Context) {
	queue := c.Param("queue")
	msg, err := h.queue.Dequeue(c, queue)
	if err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	if msg == nil {
		c.JSON(200, gin.H{}) // vacío
		return
	}
	c.JSON(200, msg)
}

func (h *Handlers) Ack(c *gin.Context) {
	queue := c.Param("queue")
	var req struct {
		ID string `json:"id"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	uid, _ := uuid.Parse(req.ID)
	if err := h.queue.Ack(c, queue, uid); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.Status(204)
}
