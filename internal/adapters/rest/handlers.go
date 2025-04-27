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
	c.JSON(http.StatusOK, gin.H{
		"token": u.User,
		"user":  u.User,
	})
}

// ---- TOPICS -----------------------------------------------------

func (h *Handlers) CreateTopic(c *gin.Context) {
	var req struct {
		Name string `json:"name"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	user := c.GetString("user")
	if err := h.admin.CreateTopic(c, req.Name, 3, user); err != nil {
		c.AbortWithStatusJSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	}
	c.Status(http.StatusCreated)
}

func (h *Handlers) ListTopics(c *gin.Context) {
	list, _ := h.admin.ListTopics(c)
	c.JSON(http.StatusOK, list)
}

func (h *Handlers) Publish(c *gin.Context) {
	topic := c.Param("topic")
	var req struct {
		Key     string `json:"key"`
		Payload string `json:"payload"`
	}
	if err := c.BindJSON(&req); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	user := c.GetString("user")
	part, off, err := h.pub.Publish(c, topic, req.Key, req.Payload, user)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"partition": part, "offset": off})
}

func (h *Handlers) Pull(c *gin.Context) {
	topic := c.Param("topic")
	group := c.DefaultQuery("group", "default") // Obtenemos el grupo de consumidores
	part, _ := strconv.Atoi(c.DefaultQuery("partition", "0"))
	max, _ := strconv.Atoi(c.DefaultQuery("max", "100"))

	// Leer los mensajes del grupo
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

	// Confirmar el commit del offset
	if err := h.consumer.Commit(c, topic, req.Group, req.Partition, req.Offset); err != nil {
		c.AbortWithStatusJSON(400, gin.H{"error": err.Error()})
		return
	}
	c.Status(204)
}

func (h *Handlers) DeleteTopic(c *gin.Context) {
	name := c.Param("topic")
	user := c.GetString("user")
	if err := h.admin.DeleteTopic(c, name, user); err != nil {
		c.AbortWithStatusJSON(403, gin.H{"error": err.Error()})
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

func (h *Handlers) ListQueues(c *gin.Context) {
	list, _ := h.admin.ListQueues(c)
	c.JSON(http.StatusOK, list)
}

func (h *Handlers) DeleteQueue(c *gin.Context) {
	name := c.Param("queue")
	user := c.GetString("user")
	if err := h.admin.DeleteQueue(c, name, user); err != nil {
		c.AbortWithStatusJSON(403, gin.H{"error": err.Error()})
		return
	}
	c.Status(204)
}
