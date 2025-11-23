package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"github.com/Mavichy/AvitoNovember/internal/config"
	"github.com/Mavichy/AvitoNovember/internal/httpapi"
	"github.com/Mavichy/AvitoNovember/internal/repository"
	"github.com/Mavichy/AvitoNovember/internal/service"
)

func main() {
	cfg := config.FromEnv()

	db, err := sql.Open("postgres", cfg.DBDSN)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	repo := repository.NewRepository(db)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := repo.Migrate(ctx); err != nil {
		log.Fatalf("failed to run migrations: %v", err)
	}

	svc := service.NewService(repo)
	handler := httpapi.NewHandler(svc)

	srv := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: handler,
	}

	go func() {
		log.Printf("server listening on :%s", cfg.HTTPPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("graceful shutdown failed: %v", err)
	}
}
