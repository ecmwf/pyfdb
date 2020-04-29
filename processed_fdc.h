int fdc_initialise_api();
int fdc_version(const char** version);
int fdc_vcs_version(const char** version);
enum FdcErrorValues {
    FDC_SUCCESS                  = 0,
    FDC_ERROR_GENERAL_EXCEPTION  = 1,
    FDC_ERROR_UNKNOWN_EXCEPTION  = 2
};
const char* fdc_error_string(int err);
typedef void (*fdc_failure_handler_t)(void* context, int error_code);
int fdc_set_failure_handler(fdc_failure_handler_t handler, void* context);
struct fdc_Key_t;
typedef struct fdc_Key_t fdc_Key_t;
struct fdc_KeySet_t {
    int numKeys;
    char *keySet[];
};
typedef struct fdc_KeySet_t fdc_KeySet_t;
struct fdc_MarsRequest_t;
typedef struct fdc_MarsRequest_t fdc_MarsRequest_t;
struct fdc_ToolRequest_t;
typedef struct fdc_ToolRequest_t fdc_ToolRequest_t;
struct fdc_ListElement_t;
typedef struct fdc_ListElement_t fdc_ListElement_t;
struct fdc_ListIterator_t;
typedef struct fdc_ListIterator_t fdc_ListIterator_t;
struct fdc_t;
typedef struct fdc_t fdc_t;
int fdc_Key_clean(fdc_Key_t* key);
int fdc_KeySet_clean(fdc_KeySet_t* keySet);
int fdc_MarsRequest_init(fdc_MarsRequest_t** req, char* str);
int fdc_MarsRequest_value(fdc_MarsRequest_t* req, char* name, char* values[], int numValues);
int fdc_MarsRequest_parse(fdc_MarsRequest_t** req, char* str);
int fdc_MarsRequest_clean(fdc_MarsRequest_t* req);
int fdc_ToolRequest_init_all(fdc_ToolRequest_t** req, fdc_KeySet_t *keys);
int fdc_ToolRequest_init_str(fdc_ToolRequest_t** req, char *str, fdc_KeySet_t *keys);
int fdc_ToolRequest_init_mars(fdc_ToolRequest_t** req, fdc_MarsRequest_t *marsReq, fdc_KeySet_t *keys);
int fdc_ToolRequest_clean(fdc_ToolRequest_t* req);
int fdc_ListElement_init(fdc_ListElement_t** el);
int fdc_ListElement_str(fdc_ListElement_t* el, char **str);
int fdc_ListElement_clean(fdc_ListElement_t** el);
int fdc_init(fdc_t** fdc);
int fdc_clean(fdc_t* fdc);
int fdc_list(fdc_ListIterator_t** it, fdc_t* fdc, const fdc_ToolRequest_t* req);
int fdc_list_next(fdc_ListIterator_t* it, bool* exist, fdc_ListElement_t** el);
int fdc_list_clean(fdc_ListIterator_t* it);
//int fdc_archive(fdc_t* fdc, fdc_Key_t key, const void* data, size_t length);
typedef long (*fdc_stream_write_t)(void* context, const void* buffer, long length);
int fdc_retrieve_to_stream(fdc_t* fdc, fdc_MarsRequest_t* req, void* handle, fdc_stream_write_t write_fn, long* bytes_encoded);
int fdc_retrieve_to_file_descriptor(fdc_t* fdc, fdc_MarsRequest_t* req, int fd, long* bytes_encoded);
int fdc_retrieve_to_buffer(fdc_t* fdc, fdc_MarsRequest_t* req, void* buffer, long length, long* bytes_encoded);
