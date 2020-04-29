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
struct fdb_Key_t;
typedef struct fdb_Key_t fdb_Key_t;
struct fdb_KeySet_t {
    int numKeys;
    char *keySet[];
};
typedef struct fdb_KeySet_t fdb_KeySet_t;
struct fdb_MarsRequest_t;
typedef struct fdb_MarsRequest_t fdb_MarsRequest_t;
struct fdb_ToolRequest_t;
typedef struct fdb_ToolRequest_t fdb_ToolRequest_t;
struct fdb_ListElement_t;
typedef struct fdb_ListElement_t fdb_ListElement_t;
struct fdb_ListIterator_t;
typedef struct fdb_ListIterator_t fdb_ListIterator_t;
struct fdb_t;
typedef struct fdb_t fdb_t;
int fdb_Key_init(fdb_Key_t** key);
int fdb_Key_set(fdb_Key_t* key, char* k, char* v);
int fdb_Key_clean(fdb_Key_t* key);
int fdb_KeySet_clean(fdb_KeySet_t* keySet);
int fdb_MarsRequest_init(fdb_MarsRequest_t** req, char* str);
int fdb_MarsRequest_value(fdb_MarsRequest_t* req, char* name, char* values[], int numValues);
int fdb_MarsRequest_parse(fdb_MarsRequest_t** req, char* str);
int fdb_MarsRequest_clean(fdb_MarsRequest_t* req);
int fdb_ToolRequest_init_all(fdb_ToolRequest_t** req, fdb_KeySet_t *keys);
int fdb_ToolRequest_init_str(fdb_ToolRequest_t** req, char *str, fdb_KeySet_t *keys);
int fdb_ToolRequest_init_mars(fdb_ToolRequest_t** req, fdb_MarsRequest_t *marsReq, fdb_KeySet_t *keys);
int fdb_ToolRequest_clean(fdb_ToolRequest_t* req);
int fdb_ListElement_init(fdb_ListElement_t** el);
int fdb_ListElement_str(fdb_ListElement_t* el, char **str);
int fdb_ListElement_clean(fdb_ListElement_t** el);
int fdb_init(fdb_t** fdb);
int fdb_clean(fdb_t* fdb);
int fdb_list(fdb_t* fdb, const fdb_ToolRequest_t* req, fdb_ListIterator_t** it);
int fdb_list_next(fdb_ListIterator_t* it, bool* exist, fdb_ListElement_t** el);
int fdb_list_clean(fdb_ListIterator_t* it);
//int fdb_archive(fdb_t* fdb, fdb_Key_t key, const void* data, size_t length);
typedef long (*fdb_stream_write_t)(void* context, const void* buffer, long length);
int fdb_retrieve_to_stream(fdb_t* fdb, fdb_MarsRequest_t* req, void* handle, fdb_stream_write_t write_fn, long* bytes_encoded);
int fdb_retrieve_to_file_descriptor(fdb_t* fdb, fdb_MarsRequest_t* req, int fd, long* bytes_encoded);
int fdb_retrieve_to_buffer(fdb_t* fdb, fdb_MarsRequest_t* req, void* buffer, long length, long* bytes_encoded);
