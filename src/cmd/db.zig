const std = @import("std");
const proto = @import("../proto.zig");
const ql2 = @import("../ql2/ql2.pb.zig");
const RethinkDB = @import("../main.zig").RethinkDB;

pub fn db(self: *RethinkDB, name: []const u8) *RethinkDB {
    const args = self.getArgs();

    self.cmd = proto.Cmd{
        .type = ql2.Term.TermType.DB,
        .datum = proto.Datum{ .string = name },
        .args = args,
    };

    return self;
}

test "Should be create the DB command correctly" {
    var alloc = std.testing.allocator;

    var r = RethinkDB.init(alloc);
    defer r.deinit();

    _ = db(&r, "néfix");

    try std.testing.expectEqualDeep(proto.Cmd{
        .type = ql2.Term.TermType.DB,
        .datum = proto.Datum{ .string = "néfix" },
    }, r.cmd.?);
}
