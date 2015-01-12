use strict;
use warnings;
use Carp;
use Data::Dumper;

my $lines = <<EOF;
SPEED
( mph)           1     2     3     4     5     6     7     8     9    10    11    12    13    14    15    TOTALS
------------------------------------------------------------------------------------------------------------------------
  1-  5          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
  6- 10          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
 11- 15          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
 16- 20          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
 21- 25          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
 26- 30          0     0     1     0     0     0     0     0     0     0     0     0     0     0     0       1
 31- 35          0     3     1     0     1     0     0     0     0     0     0     0     0     0     0       5
 36- 40          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2
 41- 45          0     5     0     0     0     0     0     0     0     0     0     0     0     0     0       5
 46- 50          0    18     3     0     7     0     0     0    15     0     2     0     0     0     0      45
 51- 55          0   126    30     4    27     1     0    17    87     1     9     0     0     2     3     307
 56- 60          0   578   123     1    64     0     0    18    81     0    18     2     0     0     1     886
 61- 65          0  2026   533    11   137     1     0     8    16     0     0     0     0     0     3    2735
 66- 70          0  1655   401     3    55     0     0     0     1     0     0     0     0     0     1    2116
 71- 75          0   756   255     0    22     1     0     1     0     0     0     0     0     0     0    1035
 76- 80          0   221    62     0     3     0     0     0     0     0     0     0     0     0     0     286
 81- 85          0    40     9     0     0     0     0     0     0     0     0     0     0     0     0      49
 86- 90          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2
 91- 95          0     0     0     0     0     0     0     0     0     0     0     0     0     0     0       0
 96-100          0     1     1     0     0     0     0     0     0     0     0     0     0     0     0       2
  > 100          0     2     2     0     0     0     0     0     0     0     0     0     0     0     0       4
------------------------------------------------------------------------------------------------------------------------
 TOTALS          0  5433  1423    19   316     3     0    44   200     1    29     2     0     2     8    7480

EOF
my @input;
while ($lines =~ /^(.*)$/mg){
  push @input , $1;
}

for my $line(@input){
  chomp $line;
  if ( $line =~ /((\d+-|>)\s*\d+)\s+(\d.*)/sxm ){
    my $output = [$1];
    push @{$output}, split( q{ },$3 );
    carp Dumper $output;
  }
}
