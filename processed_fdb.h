int fdb_initialise();
int fdb_version(const char** version);
int fdb_vcs_version(const char** version);
enum FdbErrorValues {
    FDB_SUCCESS                  = 0,
    FDB_ERROR_GENERAL_EXCEPTION  = 1,
    FDB_ERROR_UNKNOWN_EXCEPTION  = 2
};
const char* fdb_error_string(int err);
typedef void (*fdb_failure_handler_t)(void* context, int error_code);
int fdb_set_failure_handler(fdb_failure_handler_t handler, void* context);

struct fdb_key_t;
typedef struct fdb_key_t fdb_key_t;
struct fdb_request_t;
typedef struct fdb_request_t fdb_request_t;
struct fdb_listiterator_t;
typedef struct fdb_listiterator_t fdb_listiterator_t;
struct fdb_datareader_t;
typedef struct fdb_datareader_t fdb_datareader_t;
struct fdb_handle_t;
typedef struct fdb_handle_t fdb_handle_t;

int fdb_new_handle(fdb_handle_t** fdb);
int fdb_archive(fdb_handle_t* fdb, fdb_key_t* key, const char* data, size_t length);
int fdb_list(fdb_handle_t* fdb, const fdb_request_t* req, fdb_listiterator_t* it);
int fdb_retrieve(fdb_handle_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);
int fdb_delete_handle(fdb_handle_t* fdb);

int fdb_new_key(fdb_key_t** key);
int fdb_key_add(fdb_key_t* key, char* param, char* value);
int fdb_delete_key(fdb_key_t* key);

int fdb_new_request(fdb_request_t** req);
int fdb_request_add(fdb_request_t* req, char* param, char* values[], int numValues);
int fdb_delete_request(fdb_request_t* req);

int fdb_new_listiterator(fdb_listiterator_t** it);
int fdb_listiterator_next(fdb_listiterator_t* it, bool* exist, const char** str);
int fdb_delete_listiterator(fdb_listiterator_t* it);

int fdb_new_datareader(fdb_datareader_t** dr);
int fdb_datareader_open(fdb_datareader_t* dr);
int fdb_datareader_close(fdb_datareader_t* dr);
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);
int fdb_datareader_skip(fdb_datareader_t* dr, long count);
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read);
int fdb_delete_datareader(fdb_datareader_t* dr);