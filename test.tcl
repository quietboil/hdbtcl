lassign $::argv node uid pwd colorize
if {
    $node == "" ||
    $uid == "" ||
    $pwd == ""
} {
    puts stderr "  Usage: [info name] [info script] <server_node> <user_id> <password> ?-colorize?"
    return
}

lappend ::auto_path [pwd]

set colorize [expr { $colorize != {} }]
if { $colorize != "" } {
    set color(reset)     "\033\[0m"
    set color(feature)   "\033\[103;30m"
    set color(success)   "\033\[92m"
    set color(failure)   "\033\[91m"
    set color(skipped)   "\033\[93m"
} else {
    set color(reset)     ""
    set color(feature)   ""
    set color(success)   ""
    set color(failure)   ""
    set color(skipped)   ""
}

proc report { msg err } {
    if { $::colorize } {
        switch $err {
            1 {
                set type failure
            }
            3 {
                set type skipped
            }
            default {
                set type success
            }
        }
        puts [format "  %s%s%s" $::color($type) $msg $::color(reset)]
    } else {
        switch $err {
            1 {
                set res FAIL
            }
            3 {
                set res SKIP
            }
            default {
                set res OK
            }
        }
        if { $msg == "" } {
            set msg $res
            set res ""
        }
        puts [format "  %-62s%s" $msg $res]
    }
}

##
# Executes a test
# Usage:
#   describe "unit that is being tested" {
#        -prologue { code to set up the test environment }
#        -it "does something" { test case implementation that returns ok or error }
#        -it "also does something else" { ... }
#        -it "can do other things" { ... }
#        -epilogue { code to dismantle the test environment }
#   }
#   -prologue and -epilogue are optional.
##
proc describe { feature test_cases } {
    puts "$::color(feature)$feature$::color(reset)"

    set prologue_idx [lsearch $test_cases "-prologue"]
    set epilogue_idx [lsearch $test_cases "-epilogue"]

    if { $prologue_idx >= 0 } {
        set code [lindex $test_cases $prologue_idx+1]
        switch [catch $code res] {
            1 {
                report "prologue: $res" 1
                return
            }
            3 {
                report "" 3
                return
            }
        }
    }

    set num_args [llength $test_cases]
    for { set i 0 } { $i < $num_args } { incr i } {
        set opt [lindex $test_cases $i]
        switch -- $opt {
            -prologue {
                incr i
            }
            -epilogue {
                incr i
            }
            -it {
                set desc [lindex $test_cases [incr i]]
                set code [lindex $test_cases [incr i]]
                set err [catch $code res]
                report $desc $err
                if { $err == 1 } {
                    puts "    $res"
                }
            }
        }
    }

    if { $epilogue_idx >= 0 } {
        set code [lindex $test_cases $epilogue_idx+1]
        if { [catch $code res] == 1 } {
            report "epilogue: $res" 1
        }
    }
    return
}

##
# Executes assertion code
##
proc expect { title test_code } {
    set err [catch { uplevel $test_code } res]
    if { $err == 1 } {
        return -code error "expectation '$title' failed - $res"
    } elseif { $res != 1 } {
        return -code error "expectation '$title' is not met"
    }
    return
}

describe "TCL extension" {
    -it "can load the library" {
        package require hdbtcl
        expect "hdb command is created" {
            expr { [info commands "hdb"] == "hdb" }
        }
    }
    -it "can connect to the tenant database" {
        set ::conn [hdb connect -serverNode $::node -uid $::uid -pwd $::pwd]
    }
}

describe "Non-LOB data manipulation statements" {
    -prologue {
        if { ![info exists ::conn] || [info commands $::conn] == {} } {
            break
        }
        $::conn execute "
            CREATE TABLE hdbtcl_test_data
            ( id            INTEGER NOT NULL PRIMARY KEY
            , a_bigint      BIGINT
            , a_decimal     DECIMAL(9,3)
            , a_real        REAL
            , a_double      DOUBLE
            , a_boolean     BOOLEAN
            , a_varbinary   VARBINARY(64)
            , a_nvarchar    NVARCHAR(100)
            , a_date        DATE
            , a_time        TIME
            , a_seconddate  SECONDDATE
            , a_timestamp   TIMESTAMP
            )
        "
        set last_id 0
    }
    -it "can manipulate (big)integers" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_bigint) VALUES (?,?)"]
        set orig_id $last_id
        foreach value { 9223372036854775807 -9223372036854775808 0 {} } {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 4 }
        }

        set expecting([expr { $orig_id + 1 }]) 9223372036854775807
        set expecting([expr { $orig_id + 2 }]) -9223372036854775808
        set expecting([expr { $orig_id + 3 }]) 0
        set expecting([expr { $orig_id + 4 }]) {}

        set stmt [$::conn execute "SELECT id, a_bigint FROM hdbtcl_test_data WHERE a_bigint IS NOT NULL"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn prepare "SELECT id, a_bigint FROM hdbtcl_test_data WHERE a_bigint IS NULL AND id BETWEEN ? AND ?"]
        $stmt execute [expr { $orig_id + 1 }] $last_id
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 1 }
        }
    }
    -it "can manipulate decimals" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_decimal) VALUES (?,?)"]
        set orig_id $last_id
        foreach value { 987654.321 -987654.321 0 {} } {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 4 }
        }

        set expecting([expr { $orig_id + 1 }]) 987654.321
        set expecting([expr { $orig_id + 2 }]) -987654.321
        set expecting([expr { $orig_id + 3 }]) 0
        set expecting([expr { $orig_id + 4 }]) {}

        set stmt [$::conn execute "SELECT id, a_decimal FROM hdbtcl_test_data WHERE a_decimal IS NOT NULL"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn prepare "SELECT id, a_decimal FROM hdbtcl_test_data WHERE a_decimal IS NULL AND id BETWEEN ? AND ?"]
        $stmt execute [expr { $orig_id + 1 }] $last_id
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 1 }
        }
    }
    -it "can manipulate real numbers" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_real) VALUES (?,?)"]
        set orig_id $last_id
        foreach value { 3.14159265 -3.14159265 0 {} } {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 4 }
        }

        set expecting([expr { $orig_id + 1 }]) 3.14159265
        set expecting([expr { $orig_id + 2 }]) -3.14159265
        set expecting([expr { $orig_id + 3 }]) 0
        set expecting([expr { $orig_id + 4 }]) {}

        set stmt [$::conn execute "SELECT id, a_real FROM hdbtcl_test_data WHERE a_real IS NOT NULL"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                set rounded_original [expr { round($expecting($ret_id) * 10000000.0) / 10000000.0 }]
                set rounded_selected [expr { round($ret_val * 10000000.0) / 10000000.0 }]
                expr { $rounded_selected == $rounded_original }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn prepare "SELECT id, a_real FROM hdbtcl_test_data WHERE a_real IS NULL AND id BETWEEN ? AND ?"]
        $stmt execute [expr { $orig_id + 1 }] $last_id
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is approximately the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 1 }
        }
    }
    -it "can manipulate double numbers" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_double) VALUES (?,?)"]
        set orig_id $last_id
        foreach value { 3.1415926535897932 -3.1415926535897932 0 {} } {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 4 }
        }

        set expecting([expr { $orig_id + 1 }]) 3.1415926535897932
        set expecting([expr { $orig_id + 2 }]) -3.1415926535897932
        set expecting([expr { $orig_id + 3 }]) 0
        set expecting([expr { $orig_id + 4 }]) {}

        set stmt [$::conn execute "SELECT id, a_double FROM hdbtcl_test_data WHERE a_double IS NOT NULL"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                set rounded_original [expr { round($expecting($ret_id) * 1000000000000000.0) / 1000000000000000.0 }]
                set rounded_selected [expr { round($ret_val * 1000000000000000.0) / 1000000000000000.0 }]
                expr { $rounded_selected == $rounded_original }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn prepare "SELECT id, a_double FROM hdbtcl_test_data WHERE a_double IS NULL AND id BETWEEN ? AND ?"]
        $stmt execute [expr { $orig_id + 1 }] $last_id
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is approximately the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 1 }
        }
    }
    -it "can manipulate booleans" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_boolean) VALUES (?,?)"]
        set orig_id $last_id
        foreach value { 1 0 true false yes no {} } {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 7 }
        }

        set expecting([expr { $orig_id + 1 }]) 1
        set expecting([expr { $orig_id + 2 }]) 0
        set expecting([expr { $orig_id + 3 }]) 1
        set expecting([expr { $orig_id + 4 }]) 0
        set expecting([expr { $orig_id + 5 }]) 1
        set expecting([expr { $orig_id + 6 }]) 0
        set expecting([expr { $orig_id + 7 }]) {}

        set true_ids  [list [expr { $orig_id + 1 }] [expr { $orig_id + 3 }] [expr { $orig_id + 5 }]]
        set false_ids [list [expr { $orig_id + 2 }] [expr { $orig_id + 4 }] [expr { $orig_id + 6 }]]

        set stmt [$::conn execute "SELECT id, a_boolean FROM hdbtcl_test_data WHERE a_boolean = TRUE"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved id is one of the true ids" {
                expr { $ret_id in $true_ids }
            }
            expect "retrieved value matches the expected one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all true rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn execute "SELECT id, a_boolean FROM hdbtcl_test_data WHERE a_boolean = FALSE"]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved id is one of the false ids" {
                expr { $ret_id in $false_ids }
            }
            expect "retrieved value matches the expected one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all true rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }

        set stmt [$::conn execute "SELECT id, a_boolean FROM hdbtcl_test_data WHERE id > ? AND a_boolean IS NULL" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved id is the null id" {
                expr { $ret_id == [expr { $orig_id + 7 }] }
            }
            expect "retrieved value matches the expected one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all true rows have been retrieved" {
            expr { $num_rows_checked == 1 }
        }
    }
    -it "can manipulate binaries" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_varbinary) VALUES (?,?)"]
        set bin [binary format c* {1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16}]
        set orig_id $last_id
        foreach value [list $bin {}] {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 2 }
        }

        set expecting([expr { $orig_id + 1 }]) $bin
        set expecting([expr { $orig_id + 2 }]) {}

        set stmt [$::conn execute "SELECT id, a_varbinary FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 2 }
        }
    }
    -it "can manipulate strings" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_nvarchar) VALUES (?,?)"]
        set text "Ask not what your country can do for you — ask what you can do for your country."
        set orig_id $last_id
        foreach value [list $text {}] {
            $stmt execute [incr last_id] $value
            expect "a single row has been inserted" {
                expr { [$stmt get -numaffectedrows] == 1 }
            }
        }
        expect "all test rows have been inserted" {
            expr { $last_id - $orig_id == 2 }
        }

        set expecting([expr { $orig_id + 1 }]) $text
        set expecting([expr { $orig_id + 2 }]) {}

        set stmt [$::conn execute "SELECT id, a_nvarchar FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 2 }
        }
    }
    -it "can manipulate dates" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_date) VALUES (?,To_Date(?,?))"]
        set orig_id $last_id
        $stmt execute [incr last_id] "7/16/1969" "MM/DD/YYYY"
        $stmt execute [incr last_id] "JULY 20, 1969" "MONTH DD, YYYY"
        $stmt execute [incr last_id] {} {}

        set expecting([expr { $orig_id + 1 }]) "1969-07-16"
        set expecting([expr { $orig_id + 2 }]) "1969-07-20"
        set expecting([expr { $orig_id + 3 }]) {}

        set stmt [$::conn execute "SELECT id, a_date FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }
    }
    -it "can manipulate times" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_time) VALUES (?,?)"]
        set orig_id $last_id
        $stmt execute [incr last_id] "13:32:00"
        $stmt execute [incr last_id] "20:17:40"
        $stmt execute [incr last_id] {}

        set expecting([expr { $orig_id + 1 }]) "13:32:00"
        set expecting([expr { $orig_id + 2 }]) "20:17:40"
        set expecting([expr { $orig_id + 3 }]) {}

        set stmt [$::conn execute "SELECT id, a_time FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }
    }
    -it "can manipulate date-times (seconddate-s)" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_seconddate) VALUES (?,?)"]
        set orig_id $last_id
        $stmt execute [incr last_id] "1969-07-16 13:32:00"
        $stmt execute [incr last_id] "1969-07-20 20:17:40"
        $stmt execute [incr last_id] {}

        set expecting([expr { $orig_id + 1 }]) "1969-07-16 13:32:00"
        set expecting([expr { $orig_id + 2 }]) "1969-07-20 20:17:40"
        set expecting([expr { $orig_id + 3 }]) {}

        set stmt [$::conn execute "SELECT id, a_seconddate FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }
    }
    -it "can manipulate timestamps" {
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_timestamp) VALUES (?,?)"]
        set orig_id $last_id
        $stmt execute [incr last_id] "1969-07-16 13:32:00.0"
        $stmt execute [incr last_id] "1969-07-20 20:17:40.0"
        $stmt execute [incr last_id] {}

        set expecting([expr { $orig_id + 1 }]) "1969-07-16 13:32:00.000000000"
        set expecting([expr { $orig_id + 2 }]) "1969-07-20 20:17:40.000000000"
        set expecting([expr { $orig_id + 3 }]) {}

        set stmt [$::conn execute "SELECT id, a_timestamp FROM hdbtcl_test_data WHERE id > ?" $orig_id]
        set num_rows_checked 0
        while { [$stmt fetch row] } {
            lassign $row ret_id ret_val
            if { ![info exists expecting($ret_id)] } {
                error "unexpected id=$ret_id"
            }
            expect "retrieved value is the same as the inserted one" {
                expr { $ret_val == $expecting($ret_id) }
            }
            incr num_rows_checked
        }
        expect "all test rows have been retrieved" {
            expr { $num_rows_checked == 3 }
        }
    }
    -epilogue {
        $::conn execute "DROP TABLE hdbtcl_test_data"
    }
}

describe "LOB data manipulation statements" {
    -prologue {
        if { ![info exists ::conn] || [info commands $::conn] == {} } {
            break
        }
        $::conn execute "
            CREATE TABLE hdbtcl_test_data
            ( id            INTEGER NOT NULL PRIMARY KEY
            , a_blob        BLOB
            , a_nclob       NCLOB
            )
        "
        set last_id 0
    }
    -it "can save/load strings to/from LOBs" {
        set quote "Don't judge each day by the harvest you reap but by the seeds that you plant."

        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_nclob) VALUES (?,?)"]
        $stmt execute [incr last_id] $quote
        expect "a single row has been inserted" {
            expr { [$stmt get -numaffectedrows] == 1 }
        }

        set stmt [$::conn execute "SELECT a_nclob FROM hdbtcl_test_data WHERE id = ?" $last_id]
        expect "fetched LOB row" {
            $stmt fetch row
        }
        set text [lindex $row 0]
        expect "returned text is the same as inserted one" {
            expr { $text == $quote }
        }
    }
    -it "can save streams into LOBs" {
        set quote "Success is not final; failure is not fatal: It is the courage to continue that counts."
        # Create a test file
        set src [open test_data.txt w]
        puts -nonewline $src $quote
        close $src
        # Use it as a data source now
        set src [open test_data.txt r]
        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_nclob) VALUES (?,?)"]
        $stmt execute [incr last_id] $src
        expect "a single row has been inserted" {
            expr { [$stmt get -numaffectedrows] == 1 }
        }
        close $src
        file delete test_data.txt

        # Read the LOB back into a variable (see previous test)
        set stmt [$::conn execute "SELECT a_nclob FROM hdbtcl_test_data WHERE id = ?" $last_id]
        expect "fetched LOB row" {
            $stmt fetch row
        }
        set text [lindex $row 0]
        expect "returned text is the same as inserted one" {
            expr { $text == $quote }
        }
    }
    -it "can read LOB content piece by piece" {
        set quote "Twenty years from now you will be more disappointed by the things that you didn't do than by the ones you did do.\
                   So, throw off the bowlines, sail away from safe harbor, catch the trade winds in your sails. Explore, Dream, Discover."

        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_nclob) VALUES (?,?)"]
        $stmt execute [incr last_id] $quote
        expect "a single row has been inserted" {
            expr { [$stmt get -numaffectedrows] == 1 }
        }

        proc read_lob_into_state { read_state lob_chunk col_num col_name table_name table_owner } {
            # initial state is {}
            append read_state $lob_chunk
            return $read_state
        }

        set stmt [$::conn execute "SELECT a_nclob FROM hdbtcl_test_data WHERE id = ?" $last_id]
        expect "fetched LOB row" {
            $stmt fetch row -lobreadcommand read_lob_into_state
        }
        expect "LOB column in the returned row is replaced with the final read state and that state is the LOB content" {
            expr { [llength $row] == 1 && [lindex $row 0] == $quote }
        }

        set text ""
        proc read_lob_into_var { read_state lob_chunk col_num col_name table_name table_owner } {
            set var_name $read_state
            upvar $var_name lob_content
            append lob_content $lob_chunk
            return $read_state
        }

        set stmt [$::conn execute "SELECT a_nclob FROM hdbtcl_test_data WHERE id = ?" $last_id]
        expect "fetched LOB row" {
            $stmt fetch row -lobreadcommand read_lob_into_var -lobreadinitialstate text
        }
        expect "LOB column in the returned row is replaced with the final read state" {
            expr { [llength $row] == 1 && [lindex $row 0] == "text" }
        }
        expect "returned text is the same as inserted one" {
            expr { $text == $quote }
        }
    }
    -it "can save OUT LOBs into variables or streams" {
        set quote "Imagination was given to man to compensate him for what he is not, and a sense of humor was provided to console him for what he is."

        set stmt [$::conn prepare "INSERT INTO hdbtcl_test_data (id, a_nclob) VALUES (?,?)"]
        $stmt execute [incr last_id] $quote
        expect "a single row has been inserted" {
            expr { [$stmt get -numaffectedrows] == 1 }
        }

        set stmt [$::conn prepare "
            DO (IN p_id INT => ?, OUT p_lob NCLOB => ?)
            BEGIN
                SELECT a_nclob INTO p_lob FROM hdbtcl_test_data WHERE id = p_id;
            END
        "]
        set text ""
        $stmt execute $last_id text

        expect "saved text is the same as inserted one" {
            expr { $text == $quote }
        }

        set lob_out [open test_data.txt w]
        $stmt execute $last_id $lob_out
        close $lob_out

        # Check the saved data
        set f [open test_data.txt r]
        set text [read $f]
        close $f
        file delete test_data.txt

        expect "saved text is the same as inserted one" {
            expr { $text == $quote }
        }
    }
    -epilogue {
        $::conn execute "DROP TABLE hdbtcl_test_data"
    }
}

describe "Multiple results and print line" {
    -prologue {
        if { ![info exists ::conn] || [info commands $::conn] == {} } {
            break
        }
         $::conn execute "
            CREATE PROCEDURE test_proc AS
            BEGIN
                USING sqlscript_print AS print;
                SELECT 1 FROM dummy;
                print:print_line('Hello, World!');
                SELECT 2 FROM dummy;
                print:print_line('Hello, Wonderful World!');
            END
        "
   }
   -it "can retrieve multiple results" {
        set stmt [$::conn execute "CALL test_proc"]
        expect "first result first row" {
            expr { [$stmt fetch row] && [llength $row] == 1 && [lindex $row 0] == 1 }
        }
        expect "no more rows in the first result" {
            expr { ![$stmt fetch row] }
        }
        expect "there is another result" {
            $stmt nextresult
        }
        expect "second result first row" {
            expr { [$stmt fetch row] && [llength $row] == 1 && [lindex $row 0] == 2 }
        }
        expect "no more rows in the second result" {
            expr { ![$stmt fetch row] }
        }
        expect "no more results" {
            expr { ![$stmt nextresult] }
        }
   }
   -it "can retrieve server side printed lines" {
        set stmt [$::conn execute "CALL test_proc"]
        expect "there is a printed line" {
            $stmt get -printline line
        }
        expect "first printed line" {
            expr { $line == "Hello, World!" }
        }
        expect "there is another printed line" {
            $stmt get -printline line
        }
        expect "second printed line" {
            expr { $line == "Hello, Wonderful World!" }
        }
        expect "no more printed lines" {
            expr { ![$stmt get -printline line] }
        }
   }
    -epilogue {
        $::conn execute "DROP PROCEDURE test_proc"
    }
}