#####################################
#   STEP 1 build executable binary  #
#####################################
FROM golang:alpine AS builder

# Install git.
# Git is required for fetching the dependencies.
RUN apk update && apk add --no-cache git

# Рабочая дирекотория
WORKDIR /app

# Копирование информации об окружении приложения
COPY go.mod .
COPY go.sum .

# Запуск загрузки модулей указанных в go.mod
RUN go mod download

#Копирование кодов проекта
COPY . .

# Build the binary.
# Компиляция проекта
RUN CGO_ENABLED=0 GOOS=linux go build -o main

#####################################
#   STEP 2 build a small image      #
#####################################
# Загрузка еще образов дополнительно
FROM scratch

# Copy our static executable.
COPY --from=builder /app/main /app/main

# Run the hello binary.
ENTRYPOINT ["/app/main"]