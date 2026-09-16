/*
 * glibc 2.12 (RHEL/CentOS 6) compatibility shim.
 * aligned_alloc() appeared in glibc 2.16; emulate via posix_memalign.
 */
#ifndef ALIGNEDALLOC_COMPAT_H_
#define ALIGNEDALLOC_COMPAT_H_

#include <cstdlib>
#if defined(__GLIBC__) && defined(__GLIBC_MINOR__) && \
    (__GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 16))
extern "C" int posix_memalign(void** memptr, size_t alignment, size_t size);
static inline void* aligned_alloc(size_t alignment, size_t size) {
    void* ptr = nullptr;
    if (posix_memalign(&ptr, alignment, size) != 0)
        return nullptr;
    return ptr;
}
#endif

#endif
