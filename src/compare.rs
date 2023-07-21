use std::cmp::Ordering::{self, Equal, Greater, Less};

pub fn compare(left: &[u8], right: &[u8]) -> Ordering {
    let mut left = left.iter().fuse();
    let mut right = right.iter().fuse();

    let mut l;
    let mut r;
    let mut ll;
    let mut rr;

    macro_rules! to_digit {
        ($v:expr) => {
            $v.and_then(|v| {
                let v = *v as isize;
                if v >= ('0' as isize) && v <= ('9' as isize) {
                    Some(v as isize - 48)
                } else {
                    None
                }
            })
        };
    }

    macro_rules! read_left {
        () => {{
            l = left.next();
            ll = to_digit!(l);
        }};
    }

    macro_rules! read_right {
        () => {{
            r = right.next();
            rr = to_digit!(r);
        }};
    }

    macro_rules! return_unless_equal {
        ($ord:expr) => {
            match $ord {
                Equal => {}
                lastcmp => return lastcmp,
            }
        };
    }

    read_left!();
    read_right!();
    'nondigits: loop {
        match (l, r) {
            (Some(l_), Some(r_)) => match (ll, rr) {
                (Some(ll_), Some(rr_)) => {
                    if ll_ == 0 || rr_ == 0 {
                        // left-aligned matching. (`015` < `12`)
                        return_unless_equal!(ll_.cmp(&rr_));
                        'digits_left: loop {
                            read_left!();
                            read_right!();
                            match (ll, rr) {
                                (Some(ll_), Some(rr_)) => return_unless_equal!(ll_.cmp(&rr_)),
                                (Some(_), None) => return Greater,
                                (None, Some(_)) => return Less,
                                (None, None) => break 'digits_left,
                            }
                        }
                    } else {
                        // right-aligned matching. (`15` < `123`)
                        let mut lastcmp = ll_.cmp(&rr_);
                        'digits_right: loop {
                            read_left!();
                            read_right!();
                            match (ll, rr) {
                                (Some(ll_), Some(rr_)) => {
                                    // `lastcmp` is only used when there are the same number of
                                    // digits, so we only update it.
                                    if lastcmp == Equal {
                                        lastcmp = ll_.cmp(&rr_);
                                    }
                                }
                                (Some(_), None) => return Greater,
                                (None, Some(_)) => return Less,
                                (None, None) => break 'digits_right,
                            }
                        }
                        return_unless_equal!(lastcmp);
                    }
                    continue 'nondigits; // do not read from the iterators again
                }
                (_, _) => return_unless_equal!(l_.cmp(r_)),
            },
            (Some(_), None) => return Greater,
            (None, Some(_)) => return Less,
            (None, None) => return Equal,
        }
        read_left!();
        read_right!();
    }
}
