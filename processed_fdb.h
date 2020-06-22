int fdb_initialise_api();
int fdb_version(const char** version);
int fdb_vcs_version(const char** version);
enum FdcErrorValues {
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
struct fdb_t;
typedef struct fdb_t fdb_t;

int fdb_init(fdb_t** fdb);
int fdb_archive(fdb_t* fdb, fdb_key_t* key, const char* data, size_t length);
int fdb_list(fdb_t* fdb, const fdb_request_t* req, fdb_listiterator_t* it);
int fdb_retrieve(fdb_t* fdb, fdb_request_t* req, fdb_datareader_t* dr);
int fdb_clean(fdb_t* fdb);

int fdb_key_init(fdb_key_t** key);
int fdb_key_add(fdb_key_t* key, char* k, char* v);
int fdb_key_clean(fdb_key_t* key);

int fdb_request_init(fdb_request_t** req);
int fdb_request_add(fdb_request_t* req, char* name, char* values[], int numValues);
int fdb_request_clean(fdb_request_t* req);

int fdb_listiterator_init(fdb_listiterator_t** it);
int fdb_listiterator_next(fdb_listiterator_t* it, bool* exist, char* str, size_t length);
int fdb_listiterator_clean(fdb_listiterator_t* it);

int fdb_datareader_init(fdb_datareader_t** dr);
int fdb_datareader_open(fdb_datareader_t* dr);
int fdb_datareader_close(fdb_datareader_t* dr);
int fdb_datareader_tell(fdb_datareader_t* dr, long* pos);
int fdb_datareader_seek(fdb_datareader_t* dr, long pos);
int fdb_datareader_skip(fdb_datareader_t* dr, long count);
int fdb_datareader_read(fdb_datareader_t* dr, void *buf, long count, long* read);
int fdb_datareader_clean(fdb_datareader_t* dr);
