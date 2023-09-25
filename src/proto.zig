const std = @import("std");
const json = std.json;
const ql2 = @import("ql2/ql2.pb.zig");

// TODO: General error handling

pub const Datum = union(enum) {
    null,
    bool: bool,
    int: i64,
    float: f64,
    string: []const u8,
    array: []const Datum,
    object: *std.StringHashMap(Datum),

    pub fn deinit(self: Datum, alloc: std.mem.Allocator) void {
        switch (self) {
            .array => |arr| for (arr) |d| d.deinit(alloc),
            .object => |obj| {
                obj.deinit();
                alloc.destroy(obj);
            },
            else => {},
        }
    }

    pub fn jsonStringify(self: Datum, jws: anytype) !void {
        switch (self) {
            .null => try jws.write(null),
            .bool => |v| try jws.write(v),
            .int => |v| try jws.write(v),
            .float => |v| try jws.write(v),
            .string => |v| try jws.write(v),
            .array => |arr| {
                try jws.beginArray();
                try jws.write(@intFromEnum(ql2.Term.TermType.MAKE_ARRAY));
                try jws.beginArray();
                for (arr) |datum| {
                    try datum.jsonStringify(jws);
                }
                try jws.endArray();
                try jws.endArray();
            },
            .object => |obj| {
                try jws.beginObject();
                var it = obj.iterator();
                while (it.next()) |kv| {
                    try jws.objectField(kv.key_ptr.*);
                    try jws.write(kv.value_ptr.*);
                }
                try jws.endObject();
            },
        }
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: json.ParseOptions) !Datum {
        switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
            .null => return Datum{ .null = {} },
            .true => return Datum{ .bool = true },
            .false => return Datum{ .bool = false },
            .number, .allocated_number => |v| {
                if (json.isNumberFormattedLikeAnInteger(v)) {
                    return Datum{ .int = try std.fmt.parseInt(i64, v, 10) };
                } else {
                    return Datum{ .float = try std.fmt.parseFloat(f64, v) };
                }
            },
            .string, .allocated_string => |v| return Datum{ .string = v },
            .array_begin => {
                var array = std.ArrayList(Datum).init(alloc);
                defer array.deinit();

                while (try source.peekNextTokenType() != .array_end) {
                    try array.append(try Datum.jsonParse(alloc, source, options));
                }

                // Eat the array end
                switch (try source.next()) {
                    .array_end => {},
                    else => return error.UnexpectedToken,
                }

                return Datum{ .array = try array.toOwnedSlice() };
            },
            .object_begin => {
                var map = try alloc.create(std.StringHashMap(Datum));
                map.* = std.StringHashMap(Datum).init(alloc);

                while (try source.peekNextTokenType() != .object_end) {
                    switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
                        .string, .allocated_string => |k| {
                            const gop = try map.getOrPut(k);
                            if (gop.found_existing) {
                                switch (options.duplicate_field_behavior) {
                                    .use_first => {
                                        _ = try Datum.jsonParse(alloc, source, options);
                                        continue;
                                    },
                                    .@"error" => return error.DuplicateField,
                                    .use_last => {},
                                }
                            }

                            gop.value_ptr.* = try Datum.jsonParse(alloc, source, options);
                        },
                        else => unreachable,
                    }
                }

                // Eat the object end
                _ = try source.next();

                return Datum{ .object = map };
            },
            else => |v| {
                std.debug.print("{any}\n", .{v});
                @panic("AAAAA");
            },
        }
    }

    // TODO: Replace this with std.testing.expectDeepEqual
    fn equals(left: Datum, alloc: std.mem.Allocator, right: Datum) !void {
        if (!@import("builtin").is_test) @compileError("function only for testing");

        var stack = std.ArrayList(struct { *const Datum, *const Datum }).init(alloc);
        defer stack.deinit();

        try stack.append(.{ &left, &right });

        while (stack.items.len != 0) {
            // TODO: THIS!
            // const l, const r = stack.pop();
            const tuple = stack.pop();
            const l = tuple[0];
            const r = tuple[1];

            switch (l.*) {
                .null => |v| try std.testing.expectEqual(v, r.null),
                .bool => |v| try std.testing.expectEqual(v, r.bool),
                .int => |v| try std.testing.expectEqual(v, r.int),
                .float => |v| try std.testing.expectEqual(v, r.float),
                .string => |v| try std.testing.expectEqualStrings(v, r.string),
                .array => |items| if (items.len != r.array.len)
                    return error.NotEqual
                else for (items, r.array) |*left_array, *right_aray| {
                    try stack.append(.{ left_array, right_aray });
                },
                .object => |lobj| if (lobj.count() != r.object.count()) {
                    return error.NotEqual;
                } else {
                    var it = lobj.iterator();
                    while (it.next()) |entry| {
                        const rptr = r.object.getPtr(entry.key_ptr.*) orelse return error.NotEqual;
                        try stack.append(.{ entry.value_ptr, rptr });
                    }
                },
            }
        }
    }
};

// TODO: https://github.com/rethinkdb/rethinkdb/blob/v2.4.x/src/unittest/datum_test.cc
test "Should serialize the Datum correctly" {
    var alloc = std.testing.allocator;

    const cases = &.{ .{
        Datum{ .string = "nefix" },
        "\"nefix\"",
    }, .{
        Datum{ .array = &.{
            Datum{ .string = "Néfix" },
            Datum{ .null = {} },
        } },
        "[2,[\"Néfix\",null]]",
    } };

    inline for (cases) |case| {
        const datum_serialized = try json.stringifyAlloc(alloc, case[0], .{});
        defer alloc.free(datum_serialized);

        try std.testing.expectEqualStrings(case[1], datum_serialized);
    }
}

test "Should deserialize the Datum correctly" {
    var alloc = std.testing.allocator;

    const expected_datum = Datum{ .array = &.{
        Datum{ .string = "Néfix" },
        Datum{ .null = {} },
    } };

    const datum = try json.parseFromSlice(Datum, alloc, "[\"Néfix\",null]", .{});
    defer datum.deinit();

    try expected_datum.equals(alloc, datum.value);
}

pub const Cmd = struct {
    type: ql2.Term.TermType,
    datum: ?Datum = null,
    args: []const Cmd = &.{},
    // TODO: Options
    options: ?[]const Datum = null,

    pub fn deinit(self: Cmd, alloc: std.mem.Allocator) void {
        if (self.datum) |d| d.deinit(alloc);

        for (self.args) |arg| arg.deinit(alloc);
    }

    pub fn jsonStringify(self: Cmd, jws: anytype) !void {
        try jws.beginArray();
        try jws.write(@intFromEnum(self.type));

        try jws.beginArray();
        if (self.args.len != 0) {
            for (self.args) |arg| {
                try jws.write(arg);
            }
        }
        if (self.datum) |d| {
            // Avoid MAKE_ARRAY repetition
            if (self.type == ql2.Term.TermType.MAKE_ARRAY and d == .array) {
                for (d.array) |item| {
                    try jws.write(item);
                }
            } else {
                try jws.write(d);
            }
        }
        try jws.endArray();

        if (self.options) |opts| try jws.write(opts);
        try jws.endArray();
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: json.ParseOptions) !Cmd {
        switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
            .array_begin => {
                // Ensure we're parsing a RethinkDB command
                const cmd_type: ql2.Term.TermType = switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
                    .number, .allocated_number => |v| @enumFromInt(try std.fmt.parseInt(i32, v, 10)),
                    else => return error.UnexpectedToken,
                };

                var args = std.ArrayList(Cmd).init(alloc);
                defer args.deinit();

                var datum: ?Datum = null;

                // Handle RethinkDB arrays as datums
                if (cmd_type == ql2.Term.TermType.MAKE_ARRAY) {
                    datum = try Datum.jsonParse(alloc, source, options);
                } else {
                    // Eat the inner array begin
                    switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
                        .array_begin => {},
                        else => return error.UnexpectedToken,
                    }

                    while (try source.peekNextTokenType() != .array_end) {
                        // Parse args or datum, depending on what's sent
                        switch (try source.peekNextTokenType()) {
                            // If it starts an array, it's an argument
                            .array_begin => {
                                try args.append(try Cmd.jsonParse(alloc, source, options));
                            },

                            // Otherwise, it's a Datum
                            else => datum = try Datum.jsonParse(alloc, source, options),
                        }
                    }

                    // Eat the inner array end
                    switch (try source.next()) {
                        .array_end => {},
                        else => return error.UnexpectedToken,
                    }
                }

                // Eat the outer array end
                switch (try source.next()) {
                    .array_end => {},
                    else => return error.UnexpectedToken,
                }

                // TODO: Options

                return Cmd{
                    .type = cmd_type,
                    .datum = datum,
                    .args = try args.toOwnedSlice(),
                };
            },
            else => return error.UnexpectedToken,
        }
    }

    // TODO: Replace this with std.testing.expectDeepEqual
    fn equals(left: Cmd, alloc: std.mem.Allocator, right: Cmd) !void {
        if (!@import("builtin").is_test) @compileError("function only for testing");

        var stack = std.ArrayList(struct { *const Cmd, *const Cmd }).init(alloc);
        defer stack.deinit();

        try stack.append(.{ &left, &right });

        while (stack.items.len != 0) {
            // TODO: THIS!
            // const l, const r = stack.pop();
            const tuple = stack.pop();
            const l = tuple[0];
            const r = tuple[1];

            try std.testing.expectEqual(l.type, r.type);

            try std.testing.expectEqual(l.datum == null, r.datum == null);
            if (l.datum != null) {
                try l.datum.?.equals(alloc, r.datum.?);
            }

            try std.testing.expectEqual(l.args.len, r.args.len);
            for (l.args, r.args) |*left_array, *right_array| {
                try stack.append(.{ left_array, right_array });
            }

            // TODO: Options
        }
    }
};

test "Should serialize the Cmd correctly" {
    var alloc = std.testing.allocator;

    const cases = &.{
        .{
            Cmd{
                .type = ql2.Term.TermType.DB,
                .datum = Datum{ .string = "blog" },
            },
            "[14,[\"blog\"]]",
        },
        .{
            Cmd{
                .type = ql2.Term.TermType.TABLE,
                .datum = Datum{ .string = "users" },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.DB,
                        .datum = Datum{ .string = "blog" },
                    },
                },
            },
            "[15,[[14,[\"blog\"]],\"users\"]]",
        },
        .{
            Cmd{
                .type = ql2.Term.TermType.FILTER,
                .datum = Datum{ .object = blk: {
                    var map = try alloc.create(std.StringHashMap(Datum));
                    map.* = std.StringHashMap(Datum).init(alloc);
                    try map.put("name", Datum{
                        .string = "Michel",
                    });
                    break :blk map;
                } },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.TABLE,
                        .datum = Datum{ .string = "users" },
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.DB,
                                .datum = Datum{ .string = "blog" },
                            },
                        },
                    },
                },
            },
            "[39,[[15,[[14,[\"blog\"]],\"users\"]],{\"name\":\"Michel\"}]]",
        },
        .{
            Cmd{
                .type = ql2.Term.TermType.FUNC,
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.MAKE_ARRAY,
                        .datum = Datum{
                            .array = &.{
                                Datum{ .int = 1 },
                                Datum{ .int = 2 },
                                Datum{ .int = 3 },
                            },
                        },
                    },
                    Cmd{
                        .type = ql2.Term.TermType.ADD,
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 1 },
                            },
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 2 },
                            },
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 3 },
                            },
                        },
                    },
                },
            },
            "[69,[[2,[1,2,3]],[24,[[10,[1]],[10,[2]],[10,[3]]]]]]",
        },
    };

    inline for (cases) |case| {
        const cmd_serialized = try json.stringifyAlloc(alloc, case[0], .{});
        defer alloc.free(cmd_serialized);
        defer case[0].deinit(alloc);

        try std.testing.expectEqualStrings(case[1], cmd_serialized);
    }
}

test "Should deserialize the Cmd correctly" {
    var alloc = std.testing.allocator;

    const cases = &.{
        .{
            "[14,[\"blog\"]]",
            Cmd{
                .type = ql2.Term.TermType.DB,
                .datum = Datum{ .string = "blog" },
            },
        },
        .{
            "[15,[[14,[\"blog\"]],\"users\"]]",
            Cmd{
                .type = ql2.Term.TermType.TABLE,
                .datum = Datum{ .string = "users" },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.DB,
                        .datum = Datum{ .string = "blog" },
                    },
                },
            },
        },
        .{
            "[39,[[15,[[14,[\"blog\"]],\"users\"]],{\"name\":\"Michel\"}]]",
            Cmd{
                .type = ql2.Term.TermType.FILTER,
                .datum = Datum{
                    .object = blk: {
                        var map = try alloc.create(std.StringHashMap(Datum));
                        map.* = std.StringHashMap(Datum).init(alloc);
                        try map.put("name", Datum{
                            .string = "Michel",
                        });
                        break :blk map;
                    },
                },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.TABLE,
                        .datum = Datum{ .string = "users" },
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.DB,
                                .datum = Datum{ .string = "blog" },
                            },
                        },
                    },
                },
            },
        },
        .{
            "[69, [[2, [1, 2, 3]], [24, [[10, [1]], [10, [2]], [10, [3]]]]]]",
            Cmd{
                .type = ql2.Term.TermType.FUNC,
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.MAKE_ARRAY,
                        .datum = Datum{
                            .array = &.{
                                Datum{ .int = 1 },
                                Datum{ .int = 2 },
                                Datum{ .int = 3 },
                            },
                        },
                    },
                    Cmd{
                        .type = ql2.Term.TermType.ADD,
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 1 },
                            },
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 2 },
                            },
                            Cmd{
                                .type = ql2.Term.TermType.VAR,
                                .datum = Datum{ .int = 3 },
                            },
                        },
                    },
                },
            },
        },
        // TODO: This!
        // .{
        //     "[64,[69,[[2,[1,2]],[24,[[10,[1]],[10,[2]]]]]],10,20]",
        //     Cmd{
        //         .type = ql2.Term.TermType.FUNCALL,
        //         .args = &.{
        //             Cmd{
        //                 .type = ql2.Term.TermType.FUNC,
        //                 .args = &.{
        //                     Cmd{
        //                         .type = ql2.Term.TermType.MAKE_ARRAY,
        //                         .datum = Datum{
        //                             .array = &.{
        //                                 Datum{ .int = 1 },
        //                                 Datum{ .int = 2 },
        //                             },
        //                         },
        //                     },
        //                     Cmd{
        //                         .type = ql2.Term.TermType.ADD,
        //                         .args = &.{
        //                             Cmd{
        //                                 .type = ql2.Term.TermType.VAR,
        //                                 .datum = Datum{ .int = 1 },
        //                             },
        //                             Cmd{
        //                                 .type = ql2.Term.TermType.VAR,
        //                                 .datum = Datum{ .int = 2 },
        //                             },
        //                         },
        //                     },
        //                 },
        //             },
        //         },
        //     },
        // },
    };

    inline for (cases) |case| {
        const cmd = try json.parseFromSlice(Cmd, alloc, case[0], .{});
        defer cmd.deinit();
        defer case[1].deinit(alloc);

        try case[1].equals(alloc, cmd.value);
        // try std.testing.expectEqualDeep(case[1], cmd.value);
    }
}

pub const Query = struct {
    type: ql2.Query.QueryType,
    cmd: Cmd,
    global_options: ?*std.StringHashMap(Datum) = null,

    pub fn deinit(self: Query, alloc: std.mem.Allocator) void {
        self.cmd.deinit(alloc);

        if (self.global_options) |opts| {
            opts.deinit();
            alloc.destroy(opts);
        }
    }

    pub fn jsonStringify(self: Query, jws: anytype) !void {
        try jws.beginArray();
        try jws.write(@intFromEnum(self.type));
        try jws.write(self.cmd);

        if (self.global_options) |opts| {
            try jws.beginObject();
            var it = opts.iterator();
            while (it.next()) |kv| {
                try jws.objectField(kv.key_ptr.*);
                try jws.write(kv.value_ptr.*);
            }
            try jws.endObject();
        }

        try jws.endArray();
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: json.ParseOptions) !Query {
        switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
            .array_begin => {
                const query_type: ql2.Query.QueryType = switch (try source.nextAlloc(alloc, options.allocate orelse .alloc_if_needed)) {
                    .number, .allocated_number => |v| @enumFromInt(try std.fmt.parseInt(i32, v, 10)),
                    else => return error.UnexpectedToken,
                };

                const cmd = try Cmd.jsonParse(alloc, source, options);

                var global_options: ?*std.StringHashMap(Datum) = null;
                if (try source.peekNextTokenType() == .object_begin) {
                    const datum = try Datum.jsonParse(alloc, source, options);
                    global_options = datum.object;
                }

                // Eat the array end
                switch (try source.next()) {
                    .array_end => {},
                    else => return error.UnexpectedToken,
                }

                return Query{
                    .type = query_type,
                    .cmd = cmd,
                    .global_options = global_options,
                };
            },
            else => return error.UnexpectedToken,
        }
    }

    // TODO: Replace this with std.testing.expectDeepEqual
    fn equals(left: Query, alloc: std.mem.Allocator, right: Query) !void {
        if (!@import("builtin").is_test) @compileError("function only for testing");

        var stack = std.ArrayList(struct { *const Query, *const Query }).init(alloc);
        defer stack.deinit();

        try stack.append(.{ &left, &right });

        while (stack.items.len != 0) {
            // TODO: THIS!
            // const l, const r = stack.pop();
            const tuple = stack.pop();
            const l = tuple[0];
            const r = tuple[1];

            try std.testing.expectEqual(l.type, r.type);

            try l.cmd.equals(alloc, r.cmd);
            try std.testing.expectEqual(l.global_options == null, r.global_options == null);
            if (l.global_options != null) {
                try std.testing.expectEqual(l.global_options.?.count(), r.global_options.?.count());
                var it = l.global_options.?.iterator();
                while (it.next()) |entry| {
                    const rptr = r.global_options.?.getPtr(entry.key_ptr.*) orelse return error.NotEqual;
                    try entry.value_ptr.*.equals(alloc, rptr.*);
                }
            }
        }
    }
};

test "Should serialize the Query correctly" {
    var alloc = std.testing.allocator;

    const cases = &.{
        .{
            Query{ .type = ql2.Query.QueryType.START, .cmd = Cmd{
                .type = ql2.Term.TermType.FILTER,
                .datum = Datum{
                    .object = blk: {
                        var map = try alloc.create(std.StringHashMap(Datum));
                        map.* = std.StringHashMap(Datum).init(alloc);
                        try map.put("name", Datum{
                            .string = "Michel",
                        });
                        break :blk map;
                    },
                },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.TABLE,
                        .datum = Datum{ .string = "users" },
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.DB,
                                .datum = Datum{ .string = "blog" },
                            },
                        },
                    },
                },
            }, .global_options = blk: {
                var map = try alloc.create(std.StringHashMap(Datum));
                map.* = std.StringHashMap(Datum).init(alloc);
                break :blk map;
            } },
            "[1,[39,[[15,[[14,[\"blog\"]],\"users\"]],{\"name\":\"Michel\"}]],{}]",
        },
    };

    inline for (cases) |case| {
        const query_serialized = try json.stringifyAlloc(alloc, case[0], .{});
        defer alloc.free(query_serialized);
        defer case[0].deinit(alloc);

        try std.testing.expectEqualStrings(case[1], query_serialized);
    }
}

test "Should deserialize the Query correctly" {
    var alloc = std.testing.allocator;

    const cases = &.{
        .{
            "[1,[39,[[15,[[14,[\"blog\"]],\"users\"]],{\"name\":\"Michel\"}]],{}]",
            Query{ .type = ql2.Query.QueryType.START, .cmd = Cmd{
                .type = ql2.Term.TermType.FILTER,
                .datum = Datum{
                    .object = blk: {
                        var map = try alloc.create(std.StringHashMap(Datum));
                        map.* = std.StringHashMap(Datum).init(alloc);
                        try map.put("name", Datum{
                            .string = "Michel",
                        });
                        break :blk map;
                    },
                },
                .args = &.{
                    Cmd{
                        .type = ql2.Term.TermType.TABLE,
                        .datum = Datum{ .string = "users" },
                        .args = &.{
                            Cmd{
                                .type = ql2.Term.TermType.DB,
                                .datum = Datum{ .string = "blog" },
                            },
                        },
                    },
                },
            }, .global_options = blk: {
                var map = try alloc.create(std.StringHashMap(Datum));
                map.* = std.StringHashMap(Datum).init(alloc);
                break :blk map;
            } },
        },
    };

    inline for (cases) |case| {
        const cmd = try json.parseFromSlice(Query, alloc, case[0], .{});
        defer cmd.deinit();
        defer case[1].deinit(alloc);

        try case[1].equals(alloc, cmd.value);
        // try std.testing.expectEqualDeep(case[1], cmd.value);
    }
}
