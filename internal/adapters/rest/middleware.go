package rest

import (
	"net/http"

	"github.com/MateoRamirezRubio1/project_MOM/internal/ports/outbound"
	"github.com/gin-gonic/gin"
)

func AuthMiddleware(a outbound.AuthStore) gin.HandlerFunc {
	return func(c *gin.Context) {
		token := c.GetHeader("X-Token")
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing token"})
			return
		}
		user, ok := a.Validate(c, token)
		if !ok {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
			return
		}
		c.Set("user", user)
		c.Next()
	}
}
