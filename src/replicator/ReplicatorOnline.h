/* Header for ReplicatorOnline class
   Copyright (C) 2018-2026 Adam Leszczynski (aleszczynski@bersler.com)

This file is part of OpenLogReplicator.

This program is free software: you can redistribute it and/or
modify it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public
License along with this program; see the file LICENSE;
If not, see <http://www.gnu.org/licenses/>. */

#ifndef REPLICATOR_ONLINE_H_
#define REPLICATOR_ONLINE_H_

#include "Replicator.h"
#include "../common/DbTable.h"
#include "../metadata/SchemaElement.h"

namespace OpenLogReplicator {
    class DatabaseConnection;
    class DatabaseEnvironment;
    class Schema;

    class ReplicatorOnline final : public Replicator {
    protected:
        static constexpr std::string_view SQL_GET_ARCHIVE_LOG_LIST
        {
            "SELECT"
            "   NAME"
            ",  SEQUENCE#"
            ",  FIRST_CHANGE#"
            ",  NEXT_CHANGE#"
            ",  THREAD#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# >= :i"
            "   AND RESETLOGS_ID = :j"
            "   AND NAME IS NOT NULL"
            "   AND NAME NOT LIKE '+%'"
            " ORDER BY"
            "   THREAD#"
            ",  SEQUENCE#"
            ",  DEST_ID"
            ",  IS_RECOVERY_DEST_FILE DESC"
        };

        // Used with reader parameter "arch-source": "asm". Archived redo logs stay in ASM - the path returned
        // here is rewritten by "path-mapping" to a filesystem bridge (asmfs, NFS, ...).
        static constexpr std::string_view SQL_GET_ARCHIVE_LOG_LIST_ASM
        {
            "SELECT"
            "   NAME"
            ",  SEQUENCE#"
            ",  FIRST_CHANGE#"
            ",  NEXT_CHANGE#"
            ",  THREAD#"
            " FROM"
            "   SYS.V_$ARCHIVED_LOG"
            " WHERE"
            "   SEQUENCE# >= :i"
            "   AND RESETLOGS_ID = :j"
            "   AND NAME IS NOT NULL"
            "   AND NAME LIKE '+%'"
            " ORDER BY"
            "   THREAD#"
            ",  SEQUENCE#"
            ",  DEST_ID"
            ",  IS_RECOVERY_DEST_FILE DESC"
        };

        static constexpr std::string_view SQL_GET_DATABASE_INFORMATION
        {
            "SELECT"
            "   DECODE(D.LOG_MODE, 'ARCHIVELOG', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_MIN, 'NO', 0, 1)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_PK, 'YES', 1, 0)"
            ",  DECODE(D.SUPPLEMENTAL_LOG_DATA_ALL, 'YES', 1, 0)"
            ",  DECODE(TP.ENDIAN_FORMAT, 'Big', 1, 0)"
            ",  VER.BANNER"
            ",  SYS_CONTEXT('USERENV','DB_NAME')"
            ",  CURRENT_SCN"
            ",  DBTIMEZONE"
            " FROM"
            "   SYS.V_$DATABASE D"
            " JOIN"
            "   SYS.V_$TRANSPORTABLE_PLATFORM TP ON"
            "     TP.PLATFORM_NAME = D.PLATFORM_NAME"
            " JOIN"
            "   SYS.V_$VERSION VER ON"
            "     VER.BANNER LIKE '%Oracle%Database%'"
        };

        static constexpr std::string_view SQL_GET_DATABASE_INCARNATION
        {
            "SELECT"
            "   INCARNATION#"
            ",  RESETLOGS_CHANGE#"
            ",  PRIOR_RESETLOGS_CHANGE#"
            ",  STATUS"
            ",  RESETLOGS_ID"
            ",  PRIOR_INCARNATION#"
            " FROM"
            "   SYS.V_$DATABASE_INCARNATION"
        };

        static constexpr std::string_view SQL_GET_DATABASE_ROLE
        {
            "SELECT"
            "   DATABASE_ROLE"
            " FROM"
            "   SYS.V_$DATABASE"
        };

        static constexpr std::string_view SQL_GET_DATABASE_SCN
        {
            "SELECT"
            "   D.CURRENT_SCN"
            " FROM"
            "   SYS.V_$DATABASE D"
        };

        static constexpr std::string_view SQL_GET_CON_INFO
        {
            "SELECT"
            "   SYS_CONTEXT('USERENV','CON_ID')"
            ",  SYS_CONTEXT('USERENV','CON_NAME')"
            ",  NVL(SYS_CONTEXT('USERENV','CDB_NAME'), SYS_CONTEXT('USERENV','DB_NAME'))"
            ",  (SELECT P.DBID FROM SYS.V_$PDBS P WHERE P.CON_ID = SYS_CONTEXT('USERENV','CON_ID'))"
            " FROM"
            "   DUAL"
        };

        // ":i" carries the timestamp string (Metadata::startTime, e.g. "2026-09-17 17:00:00"), the format is
        // the literal. The previous form had the two TO_DATE arguments swapped and a missing closing bracket,
        // which made every "start-time" positioning fail: ORA-00907 as-is, ORA-01821 with the bracket fixed.
        static constexpr std::string_view SQL_GET_SCN_FROM_TIME
                {"SELECT TIMESTAMP_TO_SCN(TO_DATE(:i, 'YYYY-MM-DD HH24:MI:SS')) FROM DUAL"};

        static constexpr std::string_view SQL_GET_SCN_FROM_TIME_RELATIVE
                {"SELECT TIMESTAMP_TO_SCN(SYSDATE - (:i/24/3600)) FROM DUAL"};

        static constexpr std::string_view SQL_GET_SEQUENCE_FROM_SCN
        {
            "SELECT THREAD#, MAX(SEQUENCE#) FROM ("
            "  SELECT"
            "     THREAD#"
            "   ,  SEQUENCE#"
            "   FROM"
            "     SYS.V_$LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND SEQUENCE# > 0"
            " UNION"
            "  SELECT"
            "     THREAD#"
            "   ,  SEQUENCE#"
            "   FROM"
            "     SYS.V_$ARCHIVED_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND SEQUENCE# > 0"
            "     AND RESETLOGS_ID = :j)"
            " GROUP BY THREAD#"
        };

        static constexpr std::string_view SQL_GET_SEQUENCE_FROM_SCN_STANDBY
        {
            "SELECT THREAD#, MAX(SEQUENCE#) FROM ("
            "  SELECT"
            "     THREAD#"
            "   ,  SEQUENCE#"
            "   FROM"
            "     SYS.V_$STANDBY_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND SEQUENCE# > 0"
            " UNION"
            "  SELECT"
            "     THREAD#"
            "   ,  SEQUENCE#"
            "   FROM"
            "     SYS.V_$ARCHIVED_LOG"
            "   WHERE"
            "     FIRST_CHANGE# - 1 <= :i"
            "     AND SEQUENCE# > 0"
            "     AND RESETLOGS_ID = :j)"
            " GROUP BY THREAD#"
        };

        static constexpr std::string_view SQL_GET_LOGFILE_LIST
        {
            "SELECT"
            "   LF.GROUP#"
            ",  LF.MEMBER"
            ",  NVL(L.THREAD#, 1)"
            " FROM"
            "   SYS.V_$LOGFILE LF"
            "   LEFT JOIN SYS.V_$LOG L ON"
            "     L.GROUP# = LF.GROUP#"
            " WHERE"
            "   LF.TYPE = :i"
            " ORDER BY"
            "   NVL(L.THREAD#, 1) ASC"
            ",  LF.GROUP# ASC"
            ",  LF.IS_RECOVERY_DEST_FILE DESC"
            ",  LF.MEMBER ASC"
        };

        // Only answers "is there at least one archived redo log reachable from a filesystem" - the count is
        // capped at 1 so that the check stays cheap on databases archiving hundreds of GB/h. ROWNUM is used
        // instead of the 12c+ "FETCH FIRST" clause: this check runs at startup and the target platforms
        // include 11g, where "FETCH FIRST" raises ORA-00933.
        static constexpr std::string_view SQL_CHECK_FS_ARCHIVED_LOG
        {
            "SELECT COUNT(*) FROM ("
            "   SELECT 1 FROM"
            "      SYS.V_$ARCHIVED_LOG"
            "    WHERE"
            "      NAME IS NOT NULL"
            "      AND NAME NOT LIKE '+%'"
            "      AND ROWNUM <= 1)"
        };

        // Counterpart of SQL_CHECK_FS_ARCHIVED_LOG for reader parameter "arch-source": "asm".
        // ROWNUM instead of "FETCH FIRST" - see SQL_CHECK_FS_ARCHIVED_LOG (11g compatibility).
        static constexpr std::string_view SQL_CHECK_ASM_ARCHIVED_LOG
        {
            "SELECT COUNT(*) FROM ("
            "   SELECT 1 FROM"
            "      SYS.V_$ARCHIVED_LOG"
            "    WHERE"
            "      NAME IS NOT NULL"
            "      AND NAME LIKE '+%'"
            "      AND ROWNUM <= 1)"
        };

        static constexpr std::string_view SQL_GET_PARAMETER
        {
            "SELECT"
            "   VALUE"
            " FROM"
            "   SYS.V_$PARAMETER"
            " WHERE"
            "   NAME = :i"
        };

        static constexpr std::string_view SQL_GET_PROPERTY
        {
            "SELECT"
            "   PROPERTY_VALUE"
            " FROM"
            "   DATABASE_PROPERTIES"
            " WHERE"
            "   PROPERTY_NAME = :i"
        };

        static constexpr std::string_view SQL_GET_SYS_CCOL_USER
        {
            "SELECT"
            "   L.ROWID, L.CON#, L.INTCOL#, L.OBJ#, MOD(L.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(L.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.CCOL$ AS OF SCN :j L"
            " WHERE"
            "   L.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_CCOL_OBJ
        {
            "SELECT"
            "   L.ROWID, L.CON#, L.INTCOL#, L.OBJ#, MOD(L.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(L.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.CCOL$ AS OF SCN :j L"
            " WHERE"
            "   L.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_CDEF_USER
        {
            "SELECT"
            "   D.ROWID, D.CON#, D.OBJ#, D.TYPE#"
            " FROM"
            "   SYS.CDEF$ AS OF SCN :j D"
            " WHERE"
            "   D.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_CDEF_OBJ
        {
            "SELECT"
            "   D.ROWID, D.CON#, D.OBJ#, D.TYPE#"
            " FROM"
            "   SYS.CDEF$ AS OF SCN :j D"
            " WHERE"
            "   D.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_COL_USER
        {
            "SELECT"
            "   C.ROWID, C.OBJ#, C.COL#, C.SEGCOL#, C.INTCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$,"
            "   MOD(C.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(C.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.COL$ AS OF SCN :j C"
            " WHERE"
            "   C.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_COL_OBJ
        {
            "SELECT"
            "   C.ROWID, C.OBJ#, C.COL#, C.SEGCOL#, C.INTCOL#, C.NAME, C.TYPE#, C.LENGTH, C.PRECISION#, C.SCALE, C.CHARSETFORM, C.CHARSETID, C.NULL$,"
            "   MOD(C.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(C.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.COL$ AS OF SCN :j C"
            " WHERE"
            "   C.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_DEFERRED_STG_USER
        {
            "SELECT"
            "   DS.ROWID, DS.OBJ#, MOD(DS.FLAGS_STG, 18446744073709551616) AS FLAGS_STG1,"
            "   MOD(TRUNC(DS.FLAGS_STG / 18446744073709551616), 18446744073709551616) AS FLAGS_STG2"
            " FROM"
            "   SYS.DEFERRED_STG$ AS OF SCN :j DS"
            " WHERE"
            "   DS.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_DEFERRED_STG_OBJ
        {
            "SELECT"
            "   DS.ROWID, DS.OBJ#, MOD(DS.FLAGS_STG, 18446744073709551616) AS FLAGS_STG1,"
            "   MOD(TRUNC(DS.FLAGS_STG / 18446744073709551616), 18446744073709551616) AS FLAGS_STG2"
            " FROM"
            "   SYS.DEFERRED_STG$ AS OF SCN :j DS"
            " WHERE"
            "   DS.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_ECOL_USER
        {
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, E.GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_ECOL_OBJ
        {
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, E.GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_ECOL11_USER
        {
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, -1 AS GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_ECOL11_OBJ
        {
            "SELECT"
            "   E.ROWID, E.TABOBJ#, E.COLNUM, -1 AS GUARD_ID"
            " FROM"
            "   SYS.ECOL$ AS OF SCN :j E"
            " WHERE"
            "   E.TABOBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_USER
        {
            "SELECT"
            "   L.ROWID, L.OBJ#, L.COL#, L.INTCOL#, L.LOBJ#, L.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :j L"
            " WHERE"
            "   L.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_OBJ
        {
            "SELECT"
            "   L.ROWID, L.OBJ#, L.COL#, L.INTCOL#, L.LOBJ#, L.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " WHERE"
            "   L.OBJ# = :j"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_COMP_PART_USER
        {
            "SELECT"
            "   LCP.ROWID, LCP.PARTOBJ#, LCP.LOBJ#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :j L"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :k LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " WHERE"
            "   L.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :l)"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_COMP_PART_OBJ
        {
            "SELECT"
            "   LCP.ROWID, LCP.PARTOBJ#, LCP.LOBJ#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :j LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " WHERE"
            "   L.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_FRAG_USER
        {
            "SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :j L ON"
            "     O.OBJ# = L.OBJ#"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :k LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :l LF ON"
            "     LCP.PARTOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   O.OWNER# = :m"
            " UNION ALL"
            " SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :n O"
            " JOIN"
            "   SYS.LOB$ AS OF SCN :o L ON"
            "     O.OBJ# = L.OBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :p LF ON"
            "     L.LOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   O.OWNER# = :q"
        };

        static constexpr std::string_view SQL_GET_SYS_LOB_FRAG_OBJ
        {
            "SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :i L"
            " JOIN"
            "   SYS.LOBCOMPPART$ AS OF SCN :j LCP ON"
            "     LCP.LOBJ# = L.LOBJ#"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :k LF ON"
            "     LCP.PARTOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   L.OBJ# = :l"
            " UNION ALL"
            " SELECT"
            "   LF.ROWID, LF.FRAGOBJ#, LF.PARENTOBJ#, LF.TS#"
            " FROM"
            "   SYS.LOB$ AS OF SCN :m L"
            " JOIN"
            "   SYS.LOBFRAG$ AS OF SCN :n LF ON"
            "     L.LOBJ# = LF.PARENTOBJ#"
            " WHERE"
            "   L.OBJ# = :o"
        };

        static constexpr std::string_view SQL_GET_SYS_OBJ_USER
        {
            "SELECT"
            "   O.ROWID, O.OWNER#, O.OBJ#, O.DATAOBJ#, O.NAME, O.TYPE#,"
            "   MOD(O.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(O.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " WHERE"
            "   O.OWNER# = :j"
        };

        static constexpr std::string_view SQL_GET_SYS_OBJ_NAME
        {
            "SELECT"
            "   O.ROWID, O.OWNER#, O.OBJ#, O.DATAOBJ#, O.NAME, O.TYPE#,"
            "   MOD(O.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(O.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2"
            " FROM"
            "   SYS.OBJ$ AS OF SCN :i O"
            " WHERE"
            "   O.OWNER# = :j AND REGEXP_LIKE(O.NAME, :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_TAB_USER
        {
            "SELECT"
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.TS#, T.CLUCOLS,"
            "   MOD(T.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(T.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2,"
            "   MOD(T.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(T.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.TAB$ AS OF SCN :j T"
            " WHERE"
            "   T.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_TAB_OBJ
        {
            "SELECT"
            "   T.ROWID, T.OBJ#, T.DATAOBJ#, T.TS#, T.CLUCOLS,"
            "   MOD(T.FLAGS, 18446744073709551616) AS FLAGS1, MOD(TRUNC(T.FLAGS / 18446744073709551616), 18446744073709551616) AS FLAGS2,"
            "   MOD(T.PROPERTY, 18446744073709551616) AS PROPERTY1, MOD(TRUNC(T.PROPERTY / 18446744073709551616), 18446744073709551616) AS PROPERTY2"
            " FROM"
            "   SYS.TAB$ AS OF SCN :j T"
            " WHERE"
            "   T.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_TABCOMPART_USER
        {
            "SELECT"
            "   TCP.ROWID, TCP.OBJ#, TCP.DATAOBJ#, TCP.BO#"
            " FROM"
            "   SYS.TABCOMPART$ AS OF SCN :j TCP"
            " WHERE"
            "   TCP.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_TABCOMPART_OBJ
        {
            "SELECT"
            "   TCP.ROWID, TCP.OBJ#, TCP.DATAOBJ#, TCP.BO#"
            " FROM"
            "   SYS.TABCOMPART$ AS OF SCN :j TCP"
            " WHERE"
            "   TCP.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_TABPART_USER
        {
            "SELECT"
            "   TP.ROWID, TP.OBJ#, TP.DATAOBJ#, TP.BO#"
            " FROM"
            "   SYS.TABPART$ AS OF SCN :j TP"
            " WHERE"
            "   TP.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_TABPART_OBJ
        {
            "SELECT"
            "   TP.ROWID, TP.OBJ#, TP.DATAOBJ#, TP.BO#"
            " FROM"
            "   SYS.TABPART$ AS OF SCN :j TP"
            " WHERE"
            "   TP.OBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_TABSUBPART_USER
        {
            "SELECT"
            "   TSP.ROWID, TSP.OBJ#, TSP.DATAOBJ#, TSP.POBJ#"
            " FROM"
            "   SYS.TABSUBPART$ AS OF SCN :j TSP"
            " WHERE"
            "   TSP.OBJ# IN (SELECT OBJ# FROM SYS.OBJ$ AS OF SCN :i O WHERE O.OWNER# = :k)"
        };

        static constexpr std::string_view SQL_GET_SYS_TABSUBPART_OBJ
        {
            "SELECT"
            "   TSP.ROWID, TSP.OBJ#, TSP.DATAOBJ#, TSP.POBJ#"
            " FROM"
            "   SYS.TABSUBPART$ AS OF SCN :j TSP"
            " WHERE"
            "   TSP.POBJ# = :k"
        };

        static constexpr std::string_view SQL_GET_SYS_TS
        {
            "SELECT"
            "   T.ROWID, T.TS#, T.NAME, T.BLOCKSIZE"
            " FROM"
            "   SYS.TS$ AS OF SCN :i T"
        };

        static constexpr std::string_view SQL_GET_SYS_USER
        {
            "SELECT"
            "   U.ROWID, U.USER#, U.NAME, MOD(U.SPARE1, 18446744073709551616) AS SPARE11,"
            "   MOD(TRUNC(U.SPARE1 / 18446744073709551616), 18446744073709551616) AS SPARE12"
            " FROM"
            "   SYS.USER$ AS OF SCN :i U"
            " WHERE"
            "   REGEXP_LIKE(U.NAME, :j)"
        };

        // Debezium-compatible key inference: first standalone UNIQUE index of a table.
        // Indexes backing PRIMARY KEY / UNIQUE constraints are excluded -- those are handled
        // by the schema itself (SysCDef::isPK / isUnique fallback in Schema::buildMaps).
        static constexpr std::string_view SQL_GET_UNIQUE_INDEX_COLUMNS
        {
            "SELECT"
            "   IC.INDEX_NAME"
            ",  IC.COLUMN_NAME"
            " FROM"
            "   SYS.ALL_IND_COLUMNS IC"
            "   JOIN SYS.ALL_INDEXES I ON"
            "     I.OWNER = IC.INDEX_OWNER"
            "     AND I.INDEX_NAME = IC.INDEX_NAME"
            "     AND I.TABLE_OWNER = IC.TABLE_OWNER"
            "     AND I.TABLE_NAME = IC.TABLE_NAME"
            " WHERE"
            "   IC.TABLE_OWNER = :i"
            "   AND IC.TABLE_NAME = :j"
            "   AND I.UNIQUENESS = 'UNIQUE'"
            "   AND I.STATUS = 'VALID'"
            "   AND I.INDEX_TYPE = 'NORMAL'"
            "   AND NOT EXISTS ("
            "     SELECT 1 FROM SYS.ALL_CONSTRAINTS C"
            "     WHERE C.OWNER = IC.TABLE_OWNER"
            "       AND C.TABLE_NAME = IC.TABLE_NAME"
            "       AND C.CONSTRAINT_TYPE IN ('P', 'U')"
            "       AND C.INDEX_NAME = I.INDEX_NAME"
            "   )"
            " ORDER BY"
            "   IC.INDEX_NAME ASC"
            ",  IC.COLUMN_POSITION ASC"
        };

        static constexpr std::string_view SQL_GET_XDB_TTSET
        {
            "SELECT"
            "   T.ROWID, T.GUID, T.TOKSUF, T.FLAGS, T.OBJ#"
            " FROM"
            "   XDB.XDB$TTSET AS OF SCN :i T"
        };

        static constexpr std::string_view SQL_CHECK_CONNECTION
                {"SELECT 1 FROM DUAL"};

        bool standby{false};

        void positionReader() override;
        void loadDatabaseMetadata() override;
        bool checkConnection() override;
        bool checkConnectionFor(DatabaseConnection* databaseConnection);
        std::string getParameterValue(std::string parameter) const;
        std::string getPropertyValue(std::string property) const;
        // databaseConnection defaults to the main connection (dictionary side)
        void checkTableForGrants(const std::string& tableName, DatabaseConnection* databaseConnection = nullptr);
        void checkTableForGrantsFlashback(const std::string& tableName, Scn scn);
        std::string getModeName() const override;
        void verifySchema(Scn currentScn) override;
        void createSchema() override;
        void readSystemDictionariesMetadata(Schema* schema, Scn targetScn);
        void readSystemDictionariesDetails(Schema* schema, Scn targetScn, typeUser user, typeObj obj);
        void readSystemDictionaries(Schema* schema, Scn targetScn, const std::string& owner, const std::string& table, DbTable::OPTIONS options);
        void readUniqueIndexColumns(const std::string& owner, const std::string& table, std::vector<std::string>& keyCols);
        void createSchemaForTable(Scn targetScn, const std::string& owner, const std::string& table, const std::vector<std::string>& keyList,
                                  const std::string& key, SchemaElement::TAG_TYPE tagType, const std::vector<std::string>& tagList, const std::string& tag,
                                  const std::string& condition, DbTable::OPTIONS options, std::unordered_map<typeObj, std::string>& tablesUpdated);
        void updateOnlineRedoLogData() override;
        void loadIncarnations() override;
        // Requires mtxCheckpoint to be held by the caller (updateOnlineRedoLogData holds it for its whole body)
        void loadIncarnationsLocked();

    public:
        DatabaseEnvironment* env;
        DatabaseConnection* conn;
        // Optional second connection: the database that keeps the archived redo log list (typically a standby
        // that received the archived redo logs of the primary in "conn"). Only the archive log list and the
        // checks/reports that validate it are routed here - the dictionary, the database metadata and the
        // positioning stay on "conn". When "archives-server" is not configured this stays null and every
        // query keeps going through "conn", so the behavior is unchanged.
        DatabaseConnection* connArchives;
        bool keepConnection;

        ReplicatorOnline(Ctx* newCtx, void (*newArchGetLog)(Replicator* replicator), Builder* newBuilder, Metadata* newMetadata,
                         TransactionBuffer* newTransactionBuffer, std::string newAlias, std::string newDatabase, std::string newUser,
                         std::string newPassword, std::string newConnectString, std::string newArchivesUser,
                         std::string newArchivesPassword, std::string newArchivesConnectString, bool newKeepConnection);
        ~ReplicatorOnline() override;

        // Connection that holds the archived redo log list
        [[nodiscard]] DatabaseConnection* getArchivesConnection() const {
            return connArchives != nullptr ? connArchives : conn;
        }

        void goStandby() override;

        static void archGetLogOnline(Replicator* replicator);
    };
}

#endif
