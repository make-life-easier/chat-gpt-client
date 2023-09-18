package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
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
	Port   string `json:"port"`
}

type ErrorResponse struct {
	Error string `json:"error"`
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
			log.Printf("Ошибка при маршалинге JSON: %v", err)
			continue
		}

		req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestDataJSON))
		if err != nil {
			log.Printf("Ошибка при создании запроса: %v", err)
			continue
		}

		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("Ошибка при выполнении запроса: %v", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			log.Printf("Ошибка HTTP статуса: %v", resp.Status)
			continue
		}

		responseBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Ошибка при чтении ответа: %v", err)
			continue
		}

		task.Response = string(responseBody)

		stmt, err := db.Prepare("UPDATE tasks SET response = ?, processed = 1 WHERE id = ?")
		if err != nil {
			log.Printf("Ошибка при обновлении задания в базе данных: %v", err)
			continue
		}
		_, err = stmt.Exec(task.Response, task.ID)
		if err != nil {
			log.Printf("Ошибка при обновлении задания в базе данных: %v", err)
			continue
		}
	}
}

func getTaskByItemID(itemID int) (*Task, error) {
	var task Task
	err := db.QueryRow("SELECT id, item_id, prompt, response FROM tasks WHERE item_id = ?", itemID).Scan(&task.ID, &task.ItemId, &task.Prompt, &task.Response)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

func deleteTask(taskID int) error {
	_, err := db.Exec("DELETE FROM tasks WHERE id = ?", taskID)
	return err
}

func sendJSONError(w http.ResponseWriter, statusCode int, errorMsg string) {
	w.WriteHeader(statusCode)
	response := ErrorResponse{Error: errorMsg}
	json.NewEncoder(w).Encode(response)
}

func addTaskHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var task Task
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		sendJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	existingTask, err := getTaskByItemID(task.ItemId)
	if err != nil {
		sendJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}

	if existingTask != nil {
		if existingTask.Response != "" {
			sendJSONError(w, http.StatusBadRequest, "Этот товар уже имеет ответ")
			return
		}
		if err := deleteTask(existingTask.ID); err != nil {
			sendJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	stmt, err := db.Prepare("INSERT INTO tasks (prompt, item_id, response, processed) VALUES (?, ?, '', 0)")
	if err != nil {
		sendJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	result, err := stmt.Exec(task.Prompt, task.ItemId)
	if err != nil {
		sendJSONError(w, http.StatusInternalServerError, err.Error())
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
		sendJSONError(w, http.StatusBadRequest, "id параметр обязателен")
		return
	}

	var task Task
	err := db.QueryRow("SELECT id, item_id, prompt, response FROM tasks WHERE item_id=?", id).Scan(&task.ID, &task.ItemId, &task.Prompt, &task.Response)
	if err != nil {
		if err == sql.ErrNoRows {
			sendJSONError(w, http.StatusNotFound, "Task not found")
		} else {
			sendJSONError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	json.NewEncoder(w).Encode(task)
}

func getUnprocessedTasks() ([]Task, error) {
	rows, err := db.Query("SELECT id, item_id, prompt FROM tasks WHERE processed = 0")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var unprocessedTasks []Task
	for rows.Next() {
		var task Task
		if err := rows.Scan(&task.ID, &task.ItemId, &task.Prompt); err != nil {
			return nil, err
		}
		unprocessedTasks = append(unprocessedTasks, task)
	}

	return unprocessedTasks, nil
}

func main() {
	var err error

	logfile, err := os.Create("error.log")
	if err != nil {
		log.Fatal("Ошибка при создании файла логов:", err)
	}
	defer logfile.Close()

	log.SetOutput(logfile)

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

	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, "Hello!")
	})
	http.HandleFunc("/addTask", addTaskHandler)
	http.HandleFunc("/getTask", getTaskHandler)

	fmt.Println("Сервер запущен на :" + config.Port)
	err = http.ListenAndServe(":"+config.Port, nil)
	if err != nil {
		log.Fatal(err)
	}

	wg.Wait()
}
