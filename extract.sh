for i in `find ./dataStore/ -name "*.csv.xz" -type f`;
 do
  ./7zzs e -bso0 -bsp0 -o`dirname "$i"` "$i";
 done
