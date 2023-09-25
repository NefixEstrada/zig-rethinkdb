const std = @import("std");
const scram = @import("scram");
const ql2 = @import("ql2/ql2.pb.zig");

const HandshakeError = error{
    HandshakeVersionsError,
    ReqlAuthError,
} || std.mem.Allocator.Error || std.net.Stream.ReadError || std.net.Stream.WriteError || error{ EndOfStream, StreamTooLong } || std.json.ParseError(std.json.Scanner);

pub const Connection = struct {
    alloc: std.mem.Allocator,
    conn: std.net.Stream,

    pub fn connect(alloc: std.mem.Allocator, conn_opts: ConnectionOptions) !Connection {
        var conn = try std.net.tcpConnectToHost(alloc, conn_opts.host, conn_opts.port);
        try Connection.handshake(alloc, conn, conn_opts.user, conn_opts.password);

        return Connection{
            .alloc = alloc,
            .conn = conn,
        };
    }

    // TODO: Switch to the handshake error when possible
    fn handshake(alloc: std.mem.Allocator, conn: std.net.Stream, username: []const u8, password: []const u8) !void {
        const reader = conn.reader();
        var json_reader = std.json.reader(alloc, reader);
        defer json_reader.deinit();

        const writer = conn.writer();

        var client = try scram.ClientSha256.init(alloc, username, password, null);
        defer client.deinit();

        const client_first = try client.clientFirst();
        defer alloc.free(client_first);

        // Send the protocol version and the authentication client first message. We can send one right after
        // the other and then wait for both responses. This makes the handshake faster
        try writer.writeIntLittle(u32, @intFromEnum(ql2.VersionDummy.Version.V1_0));

        var json_writer = std.json.writeStream(writer, .{});
        try json_writer.write(ClientFirst{ .authentication = client_first });
        json_writer.deinit();
        try writer.writeByte('\x00');

        // Get the version response
        var versions_buf = std.ArrayList(u8).init(alloc);
        defer versions_buf.deinit();
        try reader.streamUntilDelimiter(versions_buf.writer(), '\x00', null);

        // TODO: Error string
        const versions = try std.json.parseFromSlice(VersionsResponse, alloc, versions_buf.items, .{});
        defer versions.deinit();

        if (!versions.value.success) return HandshakeError.HandshakeVersionsError;

        // Server first message
        var server_first_buf = std.ArrayList(u8).init(alloc);
        defer server_first_buf.deinit();
        try reader.streamUntilDelimiter(server_first_buf.writer(), '\x00', null);

        const server_first = std.json.parseFromSlice(ServerFirst, alloc, server_first_buf.items, .{}) catch {
            const versions_err = try std.json.parseFromSlice(ServerFirstError, alloc, server_first_buf.items, .{});
            defer versions_err.deinit();

            return error.ReqlAuthError;
        };
        defer server_first.deinit();

        // TODO: Compare min and max versions
        if (!server_first.value.success) return HandshakeError.ReqlAuthError;

        // Client final message
        const client_final = try client.clientFinal(server_first.value.authentication);
        defer alloc.free(client_final);

        json_writer = std.json.writeStream(writer, .{});
        try json_writer.write(ClientFinal{ .authentication = client_final });
        json_writer.deinit();
        try writer.writeByte('\x00');

        // Server final message
        var server_final_buf = std.ArrayList(u8).init(alloc);
        defer server_final_buf.deinit();
        try reader.streamUntilDelimiter(server_final_buf.writer(), '\x00', null);

        const server_final = try std.json.parseFromSlice(ServerFinal, alloc, server_final_buf.items, .{});
        defer server_final.deinit();

        if (!server_final.value.success) return HandshakeError.ReqlAuthError;

        try client.verify(server_final.value.authentication);
    }

    pub fn close(self: Connection) void {
        self.conn.close();
    }
};

pub const ConnectionOptions = struct {
    host: []const u8,
    port: u16 = 28015,
    user: []const u8 = "admin",
    password: []const u8 = "",
    db: ?[]const u8 = null,
};

test "should connect and handshake correctly" {
    var c = try Connection.connect(std.testing.allocator, .{ .host = "localhost" });
    defer c.close();
}

const VersionsResponse = struct {
    success: bool,
    min_protocol_version: u32,
    max_protocol_version: u32,
    server_version: []const u8,
};

const ClientFirst = struct {
    protocol_version: u8 = 0,
    authentication_method: []const u8 = "SCRAM-SHA-256",
    authentication: []const u8,
};

const ServerFirst = struct {
    success: bool,
    authentication: []const u8,
};

const ServerFirstError = struct {
    success: bool = false,
    @"error": []const u8,
    error_code: u8,
};

const ClientFinal = struct {
    authentication: []const u8,
};

const ServerFinal = struct {
    success: bool,
    authentication: []const u8,
};
