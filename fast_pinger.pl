#!/usr/bin/perl
use warnings;
use strict;
use feature qw/say/;
use Data::Dumper;
use Socket;

use DBI;
use EV;
use AnyEvent::Socket;
use AnyEvent::FastPing;

my @hosts;
my %school_id_and_ip = ();

# AnyEvent engine params
my $max_rtt = 2;
my $interval = 200;

$ENV{'NLS_LANG'} = "AMERICAN_AMERICA.AL32UTF8";



my $dbuser = 'xxx_xxx';
my $dbpass = 'XXXxxXxXxxxxxXXX';

my $db = DBI->connect("dbi:Oracle:host=XXXX;sid=XXXX", $dbuser, $dbpass, { ora_ncharset => 'AL32UTF8', ora_check_sql => 0 });
my $res = $db->selectall_arrayref(qq{select eq.id,eq.ip from xxx.equipments eq}, { Slice => {} });
foreach my $el (@{$res}) {
    # push(@hosts, parse_address($el->{IP}));
    push(@hosts, $el->{IP});
    $school_id_and_ip{$el->{IP}} = $el->{ID};
}

use Socket;
my @ranges; # contains pairs [$low_num, $high_num]

sub ip_to_num {
    return unpack("N", inet_aton($_[0]));
}
# returns ($found, $dest_index)
sub search_range {
    if (!@ranges) {return(0, 0);}
    my $ip = $_[0];
    my $in = ip_to_num($ip);
    my ($ln, $hn) = (0, $#ranges);
    if ($in < $ranges[$ln][0] - 1) {
        return(0, $ln);
    }
    if ($in > $ranges[$hn][1] + 1) {
        return(0, $hn + 1);
    }
    while ($ln <= $hn) {
        my $mn = int(($ln + $hn) / 2);
        if ($in < $ranges[$mn][0] - 1) {
            $hn = $mn - 1;
            next;
        }
        if ($in > $ranges[$mn][1] + 1) {
            $ln = $mn + 1;
            next;
        }
        if ($in == $ranges[$mn][0] - 1) {
            --$ranges[$mn][0];
        }
        elsif ($in == $ranges[$mn][1] + 1) {
            ++$ranges[$mn][1];
        }
        return(1, $mn);
    }
    return(0, $ln);
}
sub add_ip_to_ranges {
    my $ip = $_[0];
    my ($f, $ri) = search_range($ip);
    if (!$f) {
        if ($ri > $#ranges) {
            push @ranges, [ ip_to_num($ip), ip_to_num($ip) ];
        }
        else {
            for (my $di = @ranges; $di > $ri; --$di) {
                $ranges[$di] = $ranges[$di - 1];
            }
            $ranges[$ri] = [ ip_to_num($ip), ip_to_num($ip) ];
        }
    }
}
sub union_ranges {
    for (my $i = 0; $i < @ranges - 1; ++$i) {
        if ($ranges[$i][1] + 1 == $ranges[$i + 1][0]) {
            $ranges[$i][1] = $ranges[$i + 1][1];
            for (my $di = $i + 1; $di < @ranges - 1; ++$di) {
                $ranges[$di] = $ranges[$di + 1];
            }
            pop @ranges;
            --$i;
        }
    }
}
sub print_range {
    my $r = $_[0];
    say inet_ntoa(pack "N", $r->[0]) . " - " . inet_ntoa(pack "N", $r->[1]);
}

add_ip_to_ranges($_) for @hosts;
union_ranges();
# print_range($_) for @ranges;

my $done_ping = AnyEvent->condvar;
my $pinger = new AnyEvent::FastPing;
$pinger->interval(1 / $interval);
$pinger->max_rtt($max_rtt);

for (@ranges) {
    my $low_num = parse_address(inet_ntoa(pack "N", $_->[0]));
    my $high_num = parse_address(inet_ntoa(pack "N", $_->[1]));
    $pinger->add_range($low_num, $high_num, 1 / 1000);
}

$pinger->on_recv(sub {
    for (@{$_[0]}) {
        # printf "%s %g\n", (AnyEvent::Socket::format_address $_->[0]), $_->[1];
        my $ips = AnyEvent::Socket::format_address $_->[0];
        $db->do('call xxx.table_ping_p(?)', undef, ($school_id_and_ip{$ips}));
    }
});
$pinger->on_idle(sub {
    say "done\n";
    undef $pinger;
    exit;
});
$pinger->start;
$done_ping->recv;

