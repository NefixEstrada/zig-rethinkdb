const std = @import("std");
const connection = @import("connection.zig");
const proto = @import("proto.zig");

const RethinkDB = struct {};

comptime {
    std.testing.refAllDecls(connection);
    std.testing.refAllDecls(proto);
}
