# Couchbase on Docker Desktop + Go CRUD Example

## 📦 1. Install Docker Desktop

Download Docker Desktop from the official website:

* **Windows / Mac**: [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
* Install and ensure Docker Engine is running.
* Verify installation:

```bash
docker --version
```

---

## 🐳 2. Run Couchbase Server on Docker Desktop

### **Step 1: Pull Couchbase image**

```bash
docker pull couchbase
```

### **Step 2: Start Couchbase container**

Use the following working command:

```bash
docker run -d --name couchbase \
  -p 8091-8097:8091-8097 \
  -p 9123:9123 \
  -p 11207:11207 \
  -p 11210:11210 \
  -p 11280:11280 \
  -p 18091-18097:18091-18097 \
  couchbase
```

### **Step 3: Open Couchbase Web Console**

Visit:

```
http://localhost:8091
```

### **Step 4: Configure Couchbase**

* Set Username: `Administrator`
* Set Password: `password` (or your choice)
* Select **Data**, **Query**, **Index**, **Search** services
* Create a **bucket** named: `testBucket`

---

## 📚 3. Install the Go library `db-intfs`

Inside your Go project:

```bash
go get github.com/b0tShaman/db-intfs
```

---

## 🔌 4. Connect to Couchbase Using `db-intfs`

Example: initialize a Couchbase DB client.

```go
package main

import (
    "fmt"
    db "github.com/b0tShaman/db-intfs"
)

func main() {
    cfg := db.CouchbaseConfig{
        ConnStr:  "couchbase://127.0.0.1",
        Username: "Administrator",
        Password: "password",
        Bucket:   "testBucket",
    }

    cb, err := db.NewCouchbase(cfg)
    if err != nil {
        panic(err)
    }

    fmt.Println("Connected to Couchbase!")
}
```

---

## ✏️ 5. CRUD Operations Example

### **Create (Insert)**

```go
type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

u := User{ID: "user123", Name: "John Doe"}
err = cb.Insert("user123", u)
if err != nil {
    fmt.Println("Insert error:", err)
}
```

### **Read (Get)**

```go
var fetched User
err = cb.Get("user123", &fetched)
if err != nil {
    fmt.Println("Get error:", err)
}
fmt.Println("Fetched:", fetched)
```

### **Update (Replace)**

```go
u.Name = "John Updated"
err = cb.Replace("user123", u)
if err != nil {
    fmt.Println("Replace error:", err)
}
```

### **Delete**

```go
err = cb.Delete("user123")
if err != nil {
    fmt.Println("Delete error:", err)
}
```

---
