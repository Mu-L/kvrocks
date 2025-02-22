start_server {tags {"bitmap"}} {
  proc set2setbit {key str} {
      set bitlen [expr {8 * [string length $str]}]
      binary scan $str B$bitlen bit_str
      for {set x 0} {$x < $bitlen} {incr x} {
          r setbit $key $x [string index $bit_str $x]
      }
  }

  test {GET bitmap string after setbit} {
      r setbit b0 0 0
      r setbit b1 35 0
      set2setbit b2 "\xac\x81\x32\x5d\xfe"
      set2setbit b3 "\xff\xff\xff\xff"
      list [r get b0] [r get b1] [r get b2] [r get b3]
  } [list "\x00" "\x00\x00\x00\x00\x00" "\xac\x81\x32\x5d\xfe" "\xff\xff\xff\xff"]

  test {GET bitmap with out of max size} {
      r config set max-bitmap-to-string-mb 1
      r setbit b0 8388609 0
      catch {r get b0} e
      set e
  } {ERR Operation aborted: The size of the bitmap *}

  test "SETBIT/GETBIT/BITCOUNT/BITPOS boundary check (type bitmap)" {
      r del b0
      set max_offset [expr 4*1024*1024*1024-1]
      assert_error "*out of range*" {r setbit b0 [expr $max_offset+1] 1}
      r setbit b0 $max_offset 1
      assert_equal 1 [r getbit b0 $max_offset ]
      assert_equal 1 [r bitcount b0 0 [expr $max_offset / 8] ]
      assert_equal $max_offset  [r bitpos b0 1 ]
  }
}