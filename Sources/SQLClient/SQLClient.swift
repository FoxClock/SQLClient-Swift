// SQLClient.swift
// Swift conversion of https://github.com/martinrybak/SQLClient
// Updated to reflect FreeTDS 1.x (through 1.5) API additions and changes.
// MIT License

import Foundation
import CSybdb

// MARK: - FreeTDS Compatibility Constants

private let _DBSETUSER:        Int32 = 2
private let _DBSETPWD:         Int32 = 3
private let _DBSETAPP:         Int32 = 5
private let _DBSETPORT:        Int32 = 13
private let _DBSETREADONLY:    Int32 = 14
private let _DBSETNETWORKAUTH: Int32 = 15
private let _DBSETNTLMV2:      Int32 = 16
private let _DBSETENCRYPTION:  Int32 = 17
private let _DBSETUTF16:       Int32 = 18

private let _SYBMSDATE:            Int = 40
private let _SYBMSTIME:            Int = 41
private let _SYBMSDATETIME2:       Int = 42
private let _SYBMSDATETIMEOFFSET:  Int = 43
private let _SYBBIGDATETIME:       Int = 187
private let _SYBBIGTIME:           Int = 188
private let _SYBDATETIMN:          Int = 111
private let _SYBBITN:              Int = 104
private let _SYBUNIQUE:            Int = 36
private let _SYBNVARCHAR:          Int = 103
private let _SYBNCHAR:             Int = 102
private let _SYBXML:               Int = 241
private let _SYBBIGBINARY:         Int = 173
private let _SYBBIGVARBINARY:      Int = 174
private let _SYBBLOB:              Int = 167
private let _SYBMONEYN:            Int = 110

// MARK: - Notification Names

public extension Notification.Name {
    static let SQLClientError   = Notification.Name("SQLClientErrorNotification")
    static let SQLClientMessage = Notification.Name("SQLClientMessageNotification")
}

// MARK: - Notification UserInfo Keys

public enum SQLClientKey {
    public static let code     = "SQLClientCodeKey"
    public static let message  = "SQLClientMessageKey"
    public static let severity = "SQLClientSeverityKey"
}

// MARK: - Errors

public enum SQLClientError: Error {
    case alreadyConnected
    case notConnected
    case connectionFailed(String)
    case executionFailed(String)
}

// MARK: - Encryption Mode

public enum SQLClientEncryption: String {
    case off     = "off"
    case request = "request"   // opportunistic (default pre-1.x behaviour)
    case require = "require"   // always encrypt, accept any cert
    case strict  = "strict"    // TDS 8.0 – always encrypt with cert validation
}

// MARK: - Connection Options

public struct SQLClientConnectionOptions {
    public var server:      String
    public var username:    String
    public var password:    String
    public var database:    String?

    /// Explicit port number. Overrides any port embedded in `server`.
    public var port: UInt16?

    /// TLS/encryption mode. Default .request.
    public var encryption: SQLClientEncryption = .request

    /// Force NTLMv2 authentication. Defaults to true.
    public var useNTLMv2: Bool = true

    /// Kerberos / GSSAPI network authentication instead of SQL login.
    public var networkAuth: Bool = false

    /// Ask for a read-only routing connection (AG read replicas).
    public var readOnly: Bool = false

    /// Use UTF-16 encoding for the server connection (MSSQL).
    public var useUTF16: Bool = false

    /// Per-connection query timeout in seconds (0 = use global dbsettime).
    public var queryTimeout: Int = 0

    /// Per-connection login/connect timeout in seconds (0 = use default).
    public var loginTimeout: Int = 0

    public init(server: String, username: String, password: String, database: String? = nil) {
        self.server   = server
        self.username = username
        self.password = password
        self.database = database
    }
}

// MARK: - Execute Result

public struct SQLClientResult {
    public let tables: [[[String: Any]]]
    public let rowsAffected: Int
}

// MARK: - SQLClient

public final class SQLClient {

    public static let shared = SQLClient()
    private init() {}

    private static var isDbInitCalled = false

    public var maxTextSize: Int = 4096

    public var workerQueue: OperationQueue = {
        let q = OperationQueue()
        q.maxConcurrentOperationCount = 1
        q.name = "SQLClientWorkerQueue"
        return q
    }()

    public var callbackQueue: OperationQueue = {
        let q = OperationQueue()
        q.maxConcurrentOperationCount = 4
        q.name = "SQLClientCallbackQueue"
        return q
    }()

    private var login:      OpaquePointer?
    private var connection: OpaquePointer?
    public private(set) var isConnected = false

    // MARK: - Connect

    public func connect(
        server:     String,
        username:   String,
        password:   String,
        database:   String? = nil,
        completion: @escaping (Bool) -> Void
    ) {
        let opts = SQLClientConnectionOptions(server: server, username: username,
                                             password: password, database: database)
        connect(options: opts, completion: completion)
    }

    public func connect(
        options: SQLClientConnectionOptions,
        completion: @escaping (Bool) -> Void
    ) {
        workerQueue.addOperation { [weak self] in
            guard let self = self else { return }
            let success = self._connect(options: options)
            self.callbackQueue.addOperation { completion(success) }
        }
    }

    public func disconnect() {
        workerQueue.addOperation { [weak self] in self?._disconnect() }
    }

    // MARK: - Execute

    public func execute(
        _ command: String,
        completion: @escaping ([[[String: Any]]]) -> Void
    ) {
        executeWithResult(command) { result in
            completion(result.tables)
        }
    }

    public func executeWithResult(
        _ command: String,
        completion: @escaping (SQLClientResult) -> Void
    ) {
        workerQueue.addOperation { [weak self] in
            guard let self = self else { return }
            let result = self._execute(command: command)
            self.callbackQueue.addOperation { completion(result) }
        }
    }

    // MARK: - Private: Connect

    private func _connect(options: SQLClientConnectionOptions) -> Bool {
        guard !isConnected else {
            postError(code: 0, message: "Already connected.", severity: 0)
            return false
        }

        if ProcessInfo.processInfo.environment["TDSVER"] == nil {
            setenv("TDSVER", "7.4", 1)
        }

        if !SQLClient.isDbInitCalled {
            if dbinit() == FAIL {
                return false
            }
            SQLClient.isDbInitCalled = true
        }

        dberrhandle(SQLClient_errorHandler)
        dbmsghandle(SQLClient_messageHandler)

        guard let lgn = dblogin() else {
            postError(code: 0, message: "Could not allocate login record.", severity: 0)
            return false
        }
        self.login = lgn

        dbsetlname(lgn, options.username, _DBSETUSER)
        dbsetlname(lgn, options.password, _DBSETPWD)
        dbsetlname(lgn, "SQLClient", _DBSETAPP)

        // Only set advanced fields if they are non-default or explicitly requested.
        // This avoids "Attempt to set unknown LOGINREC field" on older/Sybase-mode FreeTDS.
        
        if let port = options.port {
            dbsetlshort(lgn, Int32(port), _DBSETPORT)
        }

        if options.encryption != .request {
            dbsetlname(lgn, options.encryption.rawValue, _DBSETENCRYPTION)
        }

        if !options.useNTLMv2 {
            dbsetlbool(lgn, 0, _DBSETNTLMV2)
        }

        if options.networkAuth {
            dbsetlbool(lgn, 1, _DBSETNETWORKAUTH)
        }

        if options.readOnly {
            dbsetlbool(lgn, 1, _DBSETREADONLY)
        }

        if options.useUTF16 {
            dbsetlbool(lgn, 1, _DBSETUTF16)
        }

        if options.loginTimeout > 0 {
            dbsetlogintime(Int32(options.loginTimeout))
        }

        guard let conn = dbopen(lgn, options.server) else {
            dbloginfree(lgn)
            self.login = nil
            postError(code: 0, message: "Could not connect to '\(options.server)'.", severity: 0)
            return false
        }
        self.connection = conn

        _ = dbsetopt(conn, DBTEXTSIZE, "\(maxTextSize)", -1)

        if options.queryTimeout > 0 {
            _ = dbsetopt(conn, DBSETTIME, "\(options.queryTimeout)", -1)
        }

        if let db = options.database, !db.isEmpty {
            guard dbuse(conn, db) != FAIL else {
                _disconnect()
                postError(code: 0, message: "Could not use database '\(db)'.", severity: 0)
                return false
            }
        }

        isConnected = true
        return true
    }

    private func _disconnect() {
        if let conn = connection { dbclose(conn);    self.connection = nil }
        if let lgn  = login     { dbloginfree(lgn); self.login = nil }
        isConnected = false
        dbexit()
    }

    private func _execute(command: String) -> SQLClientResult {
        guard isConnected, let dbproc = connection else {
            postError(code: 0, message: "Not connected.", severity: 0)
            return SQLClientResult(tables: [], rowsAffected: -1)
        }

        guard dbcmd(dbproc, command) != FAIL,
              dbsqlexec(dbproc) != FAIL else {
            postError(code: 0, message: "Could not execute command.", severity: 0)
            return SQLClientResult(tables: [], rowsAffected: -1)
        }

        var tables: [[[String: Any]]] = []
        var totalAffected: Int = -1

        var resultCode = dbresults(dbproc)
        while resultCode != NO_MORE_RESULTS && resultCode != FAIL {

            let affected = Int(dbcount(dbproc))
            if affected >= 0 {
                totalAffected = (totalAffected < 0) ? affected : totalAffected + affected
            }

            let columnCount = Int(dbnumcols(dbproc))
            var table: [[String: Any]] = []

            if columnCount > 0 {
                var columns: [(name: String, type: Int32, size: Int32)] = []
                for i in 1...columnCount {
                    let name = String(cString: dbcolname(dbproc, Int32(i)))
                    let type = dbcoltype(dbproc, Int32(i))
                    let size = dbcollen(dbproc, Int32(i))
                    columns.append((name, type, size))
                }

                while dbnextrow(dbproc) != NO_MORE_ROWS {
                    var row: [String: Any] = [:]
                    for (idx, col) in columns.enumerated() {
                        let colIdx = Int32(idx + 1)
                        row[col.name] = columnValue(conn: dbproc, column: colIdx,
                                                    type: col.type, size: col.size)
                    }
                    table.append(row)
                }
            }

            tables.append(table)
            resultCode = dbresults(dbproc)
        }

        return SQLClientResult(tables: tables, rowsAffected: totalAffected)
    }

    private func columnValue(conn: OpaquePointer, column: Int32, type: Int32, size: Int32) -> Any {
        guard let dataPtr = dbdata(conn, column) else { return NSNull() }
        let dataLen = dbdatlen(conn, column)
        guard dataLen > 0 else { return NSNull() }

        let rawData = UnsafeRawPointer(dataPtr)

        switch Int(type) {
        case SYBINT1:   return NSNumber(value: rawData.load(as: UInt8.self))
        case SYBINT2:   return NSNumber(value: rawData.load(as: Int16.self))
        case SYBINT4:   return NSNumber(value: rawData.load(as: Int32.self))
        case SYBINT8:   return NSNumber(value: rawData.load(as: Int64.self))
        case SYBREAL:   return NSNumber(value: rawData.load(as: Float.self))
        case SYBFLT8:   return NSNumber(value: rawData.load(as: Double.self))
        case SYBBIT, _SYBBITN:
            return NSNumber(value: rawData.load(as: UInt8.self) != 0)
        case SYBCHAR, SYBVARCHAR,
             _SYBNCHAR, _SYBNVARCHAR,
             SYBTEXT, SYBNTEXT, _SYBXML:
            let buf = UnsafeBufferPointer(start: dataPtr, count: Int(dataLen))
            return String(bytes: buf, encoding: .utf8)
                ?? String(bytes: buf, encoding: .windowsCP1252)
                ?? ""
        case SYBBINARY, SYBVARBINARY, SYBIMAGE, _SYBBIGBINARY, _SYBBIGVARBINARY, _SYBBLOB:
            return Data(bytes: dataPtr, count: Int(dataLen))
        case SYBDATETIME, SYBDATETIME4, _SYBDATETIMN:
            var dbdt = DBDATETIME()
            withUnsafeMutablePointer(to: &dbdt) { ptr in
                _ = dbconvert(conn, Int32(type), dataPtr, dataLen, Int32(SYBDATETIME),
                          ptr.withMemoryRebound(to: BYTE.self, capacity: MemoryLayout<DBDATETIME>.size) { $0 }, 
                          Int32(MemoryLayout<DBDATETIME>.size))
            }
            var di = DBDATEREC()
            dbdatecrack(conn, &di, &dbdt)
            return dateFrom(rec: di)
        case _SYBMSDATE, _SYBMSTIME, _SYBMSDATETIME2, _SYBMSDATETIMEOFFSET, _SYBBIGDATETIME, _SYBBIGTIME:
            return msDateTimeValue(conn: conn, column: column, type: type, data: dataPtr, dataLen: dataLen)
        case SYBDECIMAL, SYBNUMERIC:
            return convertToDecimal(conn: conn, type: type, data: dataPtr, dataLen: dataLen)
        case SYBMONEY, SYBMONEY4, _SYBMONEYN:
            return convertToDecimal(conn: conn, type: type, data: dataPtr, dataLen: dataLen)
        case _SYBUNIQUE:
            guard dataLen == 16 else { return NSNull() }
            var uuidBytes = [UInt8](repeating: 0, count: 16)
            memcpy(&uuidBytes, dataPtr, 16)
            return NSUUID(uuidBytes: &uuidBytes) as UUID
        case SYBVOID:
            return NSNull()
        default:
            return Data(bytes: dataPtr, count: Int(dataLen))
        }
    }

    private func convertToDecimal(conn: OpaquePointer, type: Int32,
                                   data: UnsafePointer<BYTE>, dataLen: Int32) -> NSDecimalNumber {
        var buf = [CChar](repeating: 0, count: 64)
        _ = dbconvert(conn, type, data, dataLen, Int32(SYBCHAR),
                  buf.withUnsafeMutableBufferPointer { $0.baseAddress?.withMemoryRebound(to: BYTE.self, capacity: 64) { $0 } }, 
                  Int32(buf.count))
        return NSDecimalNumber(string: String(cString: buf).trimmingCharacters(in: .whitespaces))
    }

    private func msDateTimeValue(conn: OpaquePointer, column: Int32, type: Int32,
                                  data: UnsafePointer<BYTE>, dataLen: Int32) -> Any {
        var buf = [CChar](repeating: 0, count: 64)
        let rc = dbconvert(conn, type, data, dataLen, Int32(SYBCHAR),
                           buf.withUnsafeMutableBufferPointer { $0.baseAddress?.withMemoryRebound(to: BYTE.self, capacity: 64) { $0 } }, 
                           Int32(buf.count))
        guard rc != FAIL else { return NSNull() }
        let str = String(cString: buf).trimmingCharacters(in: .whitespaces)
        for fmt in SQLClient.isoFormatters {
            if let date = fmt.date(from: str) { return date }
        }
        return str
    }

    private static let isoFormatters: [DateFormatter] = {
        let formats = [
            "yyyy-MM-dd HH:mm:ss.SSSSSSS", "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", "HH:mm:ss.SSSSSSS", "HH:mm:ss"
        ]
        return formats.map { fmt -> DateFormatter in
            let df = DateFormatter()
            df.locale = Locale(identifier: "en_US_POSIX")
            df.dateFormat = fmt
            return df
        }
    }()

    private func dateFrom(rec: DBDATEREC) -> Date {
        var comps = DateComponents()
        comps.year = Int(rec.dateyear); comps.month = Int(rec.datemonth) + 1; comps.day = Int(rec.datedmonth)
        comps.hour = Int(rec.datehour); comps.minute = Int(rec.dateminute); comps.second = Int(rec.datesecond)
        comps.nanosecond = Int(rec.datemsecond) * 1_000_000
        return Calendar(identifier: .gregorian).date(from: comps) ?? Date()
    }

    fileprivate func postError(code: Int, message: String, severity: Int) {
        NotificationCenter.default.post(
            name: .SQLClientError, object: self,
            userInfo: [SQLClientKey.code: code, SQLClientKey.message: message, SQLClientKey.severity: severity])
    }
}

// MARK: - Async/Await Support

public extension SQLClient {
    func connect(options: SQLClientConnectionOptions) async -> Bool {
        await withCheckedContinuation { continuation in connect(options: options) { success in continuation.resume(returning: success) } }
    }
    func connect(server: String, username: String, password: String, database: String? = nil) async -> Bool {
        await withCheckedContinuation { continuation in connect(server: server, username: username, password: password, database: database) { success in continuation.resume(returning: success) } }
    }
    func execute(_ command: String) async -> [[[String: Any]]] {
        await withCheckedContinuation { continuation in execute(command) { results in continuation.resume(returning: results) } }
    }
    func executeWithResult(_ command: String) async -> SQLClientResult {
        await withCheckedContinuation { continuation in executeWithResult(command) { result in continuation.resume(returning: result) } }
    }
}

// MARK: - FreeTDS C Callbacks

private func SQLClient_errorHandler(dbproc: OpaquePointer?, severity: Int32, dberr: Int32, oserr: Int32, dberrstr: UnsafeMutablePointer<CChar>?, oserrstr: UnsafeMutablePointer<CChar>?) -> Int32 {
    let message = dberrstr.map { String(cString: $0) } ?? "Unknown error"
    let osMsg = oserrstr.map { String(cString: $0) } ?? "No OS error"
    print("SQLClient Error [Code: \(dberr)]: \(message) (OS Error: \(osMsg))")
    NotificationCenter.default.post(name: .SQLClientError, object: nil, userInfo: [SQLClientKey.code: Int(dberr), SQLClientKey.message: message, SQLClientKey.severity: Int(severity)])
    return INT_CANCEL
}

private func SQLClient_messageHandler(dbproc: OpaquePointer?, msgno: DBINT, msgstate: Int32, severity: Int32, msgtext: UnsafeMutablePointer<CChar>?, srvname: UnsafeMutablePointer<CChar>?, proc: UnsafeMutablePointer<CChar>?, line: Int32) -> Int32 {
    let message = msgtext.map { String(cString: $0) } ?? ""
    print("SQLClient Message [No: \(msgno)]: \(message)")
    NotificationCenter.default.post(name: .SQLClientMessage, object: nil, userInfo: [SQLClientKey.code: Int(msgno), SQLClientKey.message: message, SQLClientKey.severity: Int(severity)])
    return 0
}
