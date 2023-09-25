const std = @import("std");
const ql2 = @import("ql2/ql2.pb.zig");
const connection = @import("connection.zig");
const proto = @import("proto.zig");

pub const ConnectionOptions = connection.ConnectionOptions;
pub const connect = connection.Connection.connect;

pub const RethinkDB = struct {
    alloc: std.mem.Allocator,
    conn: ?connection.Connection = null,
    cmd: ?proto.Cmd = null,

    pub fn init(alloc: std.mem.Allocator) RethinkDB {
        return RethinkDB{
            .alloc = alloc,
        };
    }

    pub fn deinit(self: RethinkDB) void {
        if (self.cmd) |cmd| {
            cmd.deinit(self.alloc);
        }

        if (self.conn) |conn| {
            conn.close();
        }
    }

    pub fn connect(self: *RethinkDB, conn_opts: ConnectionOptions) !void {
        self.conn = try connection.Connection.connect(self.alloc, conn_opts);
    }

    pub fn run(self: *RethinkDB, conn: ?connection.Connection) !void {
        var c = if (conn) |c| c else if (self.conn) |c| c else return error.NoConnection;
        const cmd = self.cmd orelse return error.NoCmd;

        try c.run(proto.Query{
            .type = ql2.Query.QueryType.START,
            .cmd = cmd,
        });

        // Cleanup
        cmd.deinit(self.alloc);
        self.cmd = null;
    }

    pub fn getArgs(self: RethinkDB) []const proto.Cmd {
        if (self.cmd) |cmd| {
            return &.{cmd};
        }

        return &.{};
    }

    usingnamespace @import("cmd/db.zig");
};

test "Should run the commands correctly" {
    var alloc = std.testing.allocator;

    var r = RethinkDB.init(alloc);
    defer r.deinit();

    try r.connect(.{ .host = "localhost" });

    try r.db("isard").run(null);
}

comptime {
    std.testing.refAllDecls(connection);
    std.testing.refAllDecls(proto);
}
