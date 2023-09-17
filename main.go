package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

type Task struct {
	ID       int    `json:"id"`
	ItemId   int    `json:"item_id"`
	Prompt   string `json:"prompt"`
	Response string `json:"response"`
}

type Config struct {
	APIKey string `json:"api_key"`
}

var db *sql.DB
var maxConcurrentRequests = 10
var requestQueue = make(chan Task, maxConcurrentRequests)
var apiKey string

func loadConfig(filename string) (Config, error) {
	var config Config
	file, err := os.Open(filename)
	if err != nil {
		return config, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&config)
	return config, err
}

func initializeDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "tasks.db")
	if err != nil {
		return nil, err
	}

	createTableSQL := `
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER,
        prompt TEXT,
        response TEXT,
        processed BOOLEAN
    );
    `

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func processQueue() {
	for task := range requestQueue {
		url := "https://api.openai.com/v1/chat/completions"

		requestData := map[string]interface{}{
			"model": "gpt-3.5-turbo",
			"messages": []map[string]string{
				{
					"role":    "user",
					"content": task.Prompt,
				},
			},
			"temperature": 0.7,
		}

		requestDataJSON, err := json.Marshal(requestData)
		if err != nil {
			log.Println("Ошибка при маршалинге JSON:", err)
			continue
		}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestDataJSON))
		if err != nil {
			log.Println("Ошибка при создании запроса:", err)
			continue
		}

		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Ошибка при выполнении запроса:", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Println("Ошибка HTTP статуса:", resp.Status)
			continue
		}

		responseBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println("Ошибка при чтении ответа:", err)
			continue
		}

		task.Response = string(responseBody)

		stmt, err := db.Prepare("UPDATE tasks SET response = ?, processed = 1 WHERE id = ?")
		if err != nil {
			log.Println("Ошибка при обновлении задания в базе данных:", err)
			continue
		}
		_, err = stmt.Exec(task.Response, task.ID)
		if err != nil {
			log.Println("Ошибка при обновлении задания в базе данных:", err)
			continue
		}
	}
}

func addTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	stmt, err := db.Prepare("INSERT INTO tasks (prompt, item_id, response, processed) VALUES (?, ?, '', 0)")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	result, err := stmt.Exec(task.Prompt, task.ItemId)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	taskID, _ := result.LastInsertId()
	task.ID = int(taskID)

	requestQueue <- task

	responseTask := Task{ID: task.ID, ItemId: task.ItemId}
	json.NewEncoder(w).Encode(responseTask)
}

func getTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "id параметр обязателен", http.StatusBadRequest)
		return
	}

	var task Task
	err := db.QueryRow("SELECT id, item_id, prompt, response FROM tasks WHERE item_id=?", id).Scan(&task.ID, &task.ItemId, &task.Prompt, &task.Response)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "Task not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	json.NewEncoder(w).Encode(task)
}

func getUnprocessedTasks() ([]Task, error) {
	rows, err := db.Query("SELECT id, prompt FROM tasks WHERE processed = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var unprocessedTasks []Task
	for rows.Next() {
		var task Task
		if err := rows.Scan(&task.ID, &task.Prompt); err != nil {
			return nil, err
		}
		unprocessedTasks = append(unprocessedTasks, task)
	}

	return unprocessedTasks, nil
}

func main() {
	var err error

	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Ошибка при загрузке конфигурации: %v", err)
	}

	apiKey = config.APIKey

	db, err = initializeDB()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// необработанные задания
	unprocessedTasks, err := getUnprocessedTasks()
	if err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < maxConcurrentRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			processQueue()
		}()
	}

	for _, task := range unprocessedTasks {
		requestQueue <- task
	}

	http.HandleFunc("/addTask", addTaskHandler)
	http.HandleFunc("/getTask", getTaskHandler)

	fmt.Println("Сервер запущен на :8080")
	err = http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()
}
