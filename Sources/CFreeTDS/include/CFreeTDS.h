#ifndef CFREETDS_H
#define CFREETDS_H

#pragma once

#ifdef __APPLE__
    #ifdef __x86_64__
    #include "/usr/local/opt/freetds/include/sybdb.h"
    #include "/usr/local/opt/freetds/include/sybfront.h"
    #else
    #include "/opt/homebrew/opt/freetds/include/sybdb.h"
    #include "/opt/homebrew/opt/freetds/include/sybfront.h"
    #endif
#else
    #include <sybdb.h>
    #include <sybfront.h>
#endif

#endif /* CFREETDS_H */
