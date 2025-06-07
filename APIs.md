# APIs

### Logs:
- **POST /logs** - adding logs
    - body template:
    ```js
    {
        "storage": string (max_len: 200),
        "logs": [
            {
                "timestamp": integer,
                "level":     integer                  (min: 0, max: 7),
                "traces":    [ string (max_len: 50) ] (max_len: 20),
                "entity":    string                   (max_len: 50),
                "entity_id": string                   (max_len: 50),
                "message":   string                   (max_len: 255),
                "modules":   [ string (max_len: 50) ] (max_len: 40),
                "labels":    [ string (max_len: 50) ] (max_len: 20, optional),
                "fields":    {
                    string (max_len: 50): string (max_len: 50)
                } (max_len: 20, optional)
            }
        ]`
    }
    ```
    - Succes:
        - `201` Created
    - Faling:
        - `400` Bad Request
        - `404` Not found

- **POST /logs/search** - searching and getting logs
    - body template (requiring of fields depends on the request):
    ```js
    {
        "storage":       string (max_len: 200),
        "select":        [ string (must be a column) ],
        "time_range":    string,
        "aggreg_values": [],
        "where":         string,
        "where_values":  [],
        "group_by":      string (must be a column),
        "having":        string,
        "having_values": [],
        "order_by":      string (must be a column),
        "limit":         integer,
        "offset":        integer,
    }
    ```
    - Succes:
        - `200` OK
    - Faling:
        - `400` Bad Request
        - `404` Not found

- **DELETE /logs** - deleting logs
    - body template:
    ```js
    {
        "storage":      string (max_len: 200),
        "time_range":   string,
        "where":        string,
        "where_values": []
    }
    ```
    - Succes:
        - `200` OK
    - Faling:
        - `400` Bad Request
        - `404` Not found

### Storages:
- **GET /storages** - getting a list of storages
    - Succes:
        - `200` OK
    - Faling:
        - `400` Bad Request

- **POST /storage** - creating storage
    - body template:
    ```js
    { "storage": string (max_len: 200) }
    ```
    - Succes:
        - `201` Created
    - Faling:
        - `400` Bad Request
        - `409` Conflict

- **DELETE /storage** - deleting storage
    - body template:
    ```js
    { "storage": string (max_len: 200) }
    ```
    - Succes:
        - `200` OK
    - Faling:
        - `400` Bad Request
        - `404` Not found

### Admin panel:
- **POST /shutdown** - shutting down the DBMS
    - body template:
    ```js
    { "password": string }
    ```
    - Succes:
        - `200` OK
    - Faling:
        - `400` Bad Request
        - `403` Forbidden