#!/usr/bin/perl
#********************************************************************************************************
# gegevens-vanuit-xaf.pl										*
#********************************************************************************************************
# Haalt gegevens uit de Snelstart journal file (XML xaf versie 3) en SnelStart database.		*
# Zorg dat er een kopie van de .xaf file gereed staat op de directory  waarin dit script gerund wordt.	*
# Ook wordt een kopie van de mdb database gebruikt.							*
# Zorg dat de .mdb file gereed staat op dezelfde directory.						*
# We gebruiken een PostgreSQL database "snelstart_<user>" voor scratch.					*
# Als parameter kan de xaf filenaam worden meegegeven.  Default is "snelstart.xaf".			*
# de .xaf file wordt eerst omgezet in een file waaruit de bijzondere tekens, zoals ë, é, & worden	*
# ge-encodeerd omdat verwerking anders niet lukt.							*
# De XAF file geldt voor een bepaald jaar.  De openstaande facturen worden opgezocht in het vooraf-	*
# gaande jaar.												*
# OUTPUT:												*
# Dit script resulteert in de volgende 4 bestanden:							*
# relaties.eb.tmp	- Bevat een lijst met debiteuren en crediteuren voor relaties.eb		*
# mutaties.eb.tmp       - Bevat alle uit te voeren transacties voor boekingen.				*
# opening.eb.tmp	- Bevat de openingsbalans en de openstaande facturen.				*
# schema.dat.tmp	- Bevat (opzet) van het schema.							*
#													*
#********************************************************************************************************
# History:												*
# 15-01-2020, ES: Eerste opzet.										*
# 19-01-2020, ES: Openingsbalans erbij.									*
# 20-01-2020, ES: Verkopen erbij.									*
# 21-01-2020, ES: Inkopen erbij.									*
#********************************************************************************************************

use strict ;
use warnings ;

use HTML::Entities ;						# Omzeilen van speciale tekens
use XML::LibXML ;						# Interpreteren van XAF file
use DBI ;							# Voor scratch Postgres database

#### BEGIN CONFIGURATIE  ####
# Enige grootboekrekeningen waarop geselecteerd wordt.
# Pas deze zonodig aan aan uw Snelstart administratie.
my $BBALNS = 2100 ;						# Tussenrekening balans
my $D_HOOG = 8043 ;						# Grootboek voor diensten hoog tarief
my $D_EU   = 8030 ;						# Grootboek voor diensten binnen EU
my $I_HOOG = 7010 ;						# Grootboek voor inkoop hoog tarief
my $I_EU   = 7015 ;						# Grootboek voor inkoop EU hoog
my $CREDITEUREN = 1500 ;					# Grootboeknummer voor Crediteuren
my $DEBITEUREN  = 1300 ;					# Grootboeknummer voor Debiteuren
my $MEMORIAAL   = 9990 ;					# Grootboeknummer voor Memoriaal

my %dagboek = (	"1110"         => "RABO-BANK", 			# Journaal ID's
		"1200"         => "ING-BANK",
		"$DEBITEUREN"  => "Debiteuren",
		"$CREDITEUREN" => "Crediteuren",
		"2100"         => "Tussenrekening balans",
		"$MEMORIAAL"   => "Memoriaal" ) ;
#### EINDE CONFIGURATIE  ####

my $user = getlogin ;						# Usernaam
my $db = "snelstart_$user" ;					# Naam tijdelijke database
my $mdbfile ;							# Naam van .mdb file
my $sql ;							# SQL query
my $sth ;							# SQL statement handle
my $rv ;							# SQL rexecute result
my @row ;							# SQL row

my $fiscalyear ;						# Jaar uit XAF file
my $ofyear ;							# Jaar voor openstaande facuren
my %klanten = () ;						# Tabel met klanten
my %leveran = () ;						# Tabel met leveranciers en code
my %reknrs  = () ;						# Omschrijvingen rekeningnummers
my $xaffilename  = 'snelstart.xaf' ;				# XAF journaal van SnelStart (default)
my $tfilename = $xaffilename . '.enc' ;				# "HTML"-encoded versie van xaf-file
my $tmpfil    = 'dc.tmp' ;					# Tijdelijke file voor sorteren
my $reltmp    = 'relaties.eb.tmp' ;				# Outputfile voor relaties
my $muttmp    = 'mutaties.eb.tmp' ;				# Outputfile voor mutaties
my $opntmp    = 'opening.eb.tmp' ;				# Outputfile voor opening.eb
my $schtmp    = 'schema.dat.tmp' ;				# Outputfile voor schema.dat (opzet)
my $linenr    = 0 ;						# Regelnummmer binnen factuur
my %open_fac  = () ;						# Lijst met (openstaande) facturen
my $gbid_deb ;							# Grootboek ID voor Debiteuren
my $balttl_C  = 0.0 ;						# Openingsbalans credit totaal
my $balttl_D  = 0.0 ;						# Openingsbalans debet totaal

sub importtable							# Import data in de snelstart database
{
  my $tbnam = $_[0] ;						# Parameter is de tabelnaam
  my $sschema = "snelstart-schema-$tbnam.sql" ;			# Filenaam snelstart table schema
  my $sedcmd ;							# Commando voor sed

  print "Bezig met conversie van $tbnam...\n" ;			# Toon activiteit
  # Maak schema voor snelstart database
  system ( "mdb-schema $mdbfile -T $tbnam > $sschema" ) ;
  system ( "sed -i 's/Long Integer/INT/g; s/Text /VARCHAR/g' $sschema" ) ;
  system ( "sed -i 's/Currency/NUMERIC(8,2)/g' $sschema" ) ;
  system ( "sed -i 's/Boolean/INT/g' $sschema" ) ;
  system ( "sed -i 's/Single/FLOAT/g' $sschema" ) ;
  system ( "sed -i 's/DateTime (Short)/timestamp/g' $sschema" ) ;
  system ( "sed -i 's/Memo.Hyperlink/VARCHAR/g' $sschema" ) ;
  system ( "sed -i '/^-\{2,\}/d; s/DROP TABLE /DROP TABLE IF EXISTS /' $sschema" ) ;
  system ( "psql $db -f $sschema >/dev/null" ) ;
  #system ( "rm $sschema" ) ;
  # Nu de data exporteren vanuit MDB en importeren in Postgres
  system ( "mdb-export -I -R ';\n' $mdbfile $tbnam > $tbnam.sql" ) ;
  # Vervang de enkele quotes in 2 keer door een enkele quote
  $sedcmd = qw|"s/'/''/g"| ;
  system ( "sed -i $sedcmd $tbnam.sql" ) ;
  $sedcmd = qw|"s/\"/'/g"| ;
  system ( "sed -i $sedcmd $tbnam.sql" ) ;
  system ( "psql $db -f $tbnam.sql  >/dev/null" ) ;
  #system ( "rm $tbnam.sql" ) ;
}

###################################################################################################
# Begin van het hoofdprogramma
###################################################################################################
#
if ( $ARGV[0] )							# Commandline parameter meegegeven?
{
  $xaffilename = $ARGV[0] ;					# Ja, dat is de naam van de xaf file
}

# Test voor Postgres driver
my @drivers = DBI->available_drivers();				# Pak de beschikbare drivers
die "No drivers found!\n" unless @drivers ;
die "No Postgres driver found!\n" unless "Pg" ~~ @drivers ;
my $dbh = DBI->connect ( "DBI:Pg:dbname=$db;host=localhost",	# Probeer te connecten
                         $user,
                         "",
                         { PrintError => 0, RaiseError => 0 } ) ;
if ( ! $dbh )
{
  print "No database, create one\n" ;
  system ( "createdb $db" ) ;
  $dbh = DBI->connect ( "DBI:Pg:dbname=$db;host=localhost",
                        $user,
                        "" ) ;
} ;
die "Cannot create database!\n" unless $dbh ;
# We zijn nu verbonden met de database.  Zoek de mdb file.
opendir ( DIR, "." ) ;        					# Ga de .mdb file zoeken
my @files = grep ( /\.mdb$/, readdir(DIR) ) ;			# Pak daartoe alle .mdb filenamen
closedir ( DIR ) ;
if ( scalar ( @files ) == 1 )					# Er mag maar 1 .mdb file aanwezig zijn
{
  $mdbfile = "\"$files[0]\"" ;					# Pak naam van de enige .mdb-file
}
else
{
  if ( scalar ( @files ) == 0 )
  {
    die "Geen MDB database gevonden op deze directory!\n" ;
  }
  else
  {
    die "Meer dan één MDB database gevonden op deze directory!\n" ;
  }
}

# Ga enige tabellen overzetten.
if ( 1 )							# 0 = Versnellen t.b.v. debug
{
  importtable ( "tblBtw" ) ;					# Neem BTW table over
  importtable ( "tblRelatie" ) ;				# Neem Relatie tabel over
  importtable ( "tblGrootboek" ) ;				# Enz.
  importtable ( "tblGrootboekFunctie" ) ;
  importtable ( "tblFactuurNummer" ) ;
  importtable ( "tblVerkoopFactuur" ) ;
  importtable ( "tblVerkoopFactuurRegel" ) ;
  importtable ( "tblVerkoopOrderRegel" ) ;
  importtable ( "tblVerkoopOrder" ) ;
  importtable ( "tblStandaardJournaalpost" ) ;
  importtable ( "tblStandaardJournaalpostRegel" ) ;
  importtable ( "tblJournaalpost" ) ;
  importtable ( "tblJournaalpostRegel" ) ;
}
#print "\n\nInfo voor de opzet van de administratie.\n" ;
#print "----------------------------------------\n" ;

#print "De volgende BTW tarieven moeten opgenomen worden in schema.dat onder 'BTW Tarieven':\n" ;
#$sql = "SELECT fldbtwid, fldomschrijving FROM tblBtw" ;
#$sth = $dbh->prepare ( $sql ) ;
#$rv = $sth->execute() or die $DBI::errstr ;
#if ( $rv < 0 )
#{
#  print $DBI::errstr ;
#}
#while ( @row = $sth->fetchrow_array())
#{
#  print "\t$row[0]\t$row[1]\n" ;
#}
#print "\n\n" ;

# Ga grootboekrekeningen verzamelen
open ( OUTPUT, ">$schtmp" ) or die "Kan $schtmp niet openen, $!" ;
print OUTPUT "# De volgende grootboekrekeningen kunnen (na edit) opgenomen worden in schema.dat:\n" ;
$sql = "SELECT b.fldnummer, b.fldomschrijving, f.fldomschrijving, b.fldgrootboekid " .
         "FROM tblGrootboek b, tblgrootboekfunctie f " .
        "WHERE b.fldGrootboekFunctieID = f.fldGrootboekFunctieID " .
          "AND b.fldnonactief = 0 " .
        "ORDER BY b.fldnummer" ;
$sth = $dbh->prepare ( $sql ) ;
$rv = $sth->execute() or die $DBI::errstr ;
if ( $rv < 0 )
{
  print $DBI::errstr ;
}
while ( @row = $sth->fetchrow_array())
{
  $reknrs{$row[0]} = $row[1] ;					# Bewaar omschrijving
  my $line = sprintf("\t%5d\t%-50s\t%s\n",
                     $row[0], $row[1], $row[2] ) ;
  if ( $row[1] eq "Debiteuren" )				# Is dit de regel voor "Debiteuren"?
  {
    $gbid_deb = $row[3] ;					# Ja, onthou de code voor later gebruik
  }
  print OUTPUT $line ;						# Regel naar outputfile
}
close ( OUTPUT ) ;
print "Grootboekrekeningen staan klaar in $schtmp\n" ;

# Ga de speciale tekens (zoals ë, é &) in de XAF file omzetten om XML te laten werken.
open ( INPUT, "<$xaffilename" ) or die "Kan $xaffilename niet openen, $!" ;
binmode INPUT ;
open ( OUTPUT, ">$tfilename" ) or die "Kan $tfilename niet openen, $!" ;
while ( <INPUT> )							# Lees de XAF file regel voor regel
{
  $linenr++ ;
  my $line = $_ ;
  if ( $linenr == 1 )
  {
    next ;								# Eerste regel helemaal weglaten
  }
  elsif ( $linenr == 2 ) 
  {
    if ( $line =~ /auditfile/ )						# Tweede regel wat inkorten
    {
      $line = '<auditfile>' ;
    }
  }
  else									# Regel 3 tot einde
  {
    $line = encode_entities ( $line, '\x80-\xFF' ) ;			# Vervang speciale tekens
    $line =~ tr/&/$/ ;							# Ampersand mag ook al niet
  }
  print OUTPUT $line ;
}
close ( INPUT ) ;
close ( OUTPUT ) ;

my $dom = XML::LibXML->load_xml(location => $tfilename)  ;		# Load de geconverteerde XML file
my $node = '/auditfile/header' ;					# Zoek jaar in header
foreach my $el ( $dom->findnodes ( $node ) )
{
  $fiscalyear = substr ( $el->findvalue ( 'fiscalYear' ), 0, 4 ) ;	# Pak het jaar
  last ;
}
$ofyear = $fiscalyear - 1 ;						# Jaar voor openstaande facturen
 
$node  = '/auditfile/company/customersSuppliers/customerSupplier' ;	# Zoek de relaties (leveranciers)
open ( OUTPUT, ">$reltmp" ) or die "Kan $reltmp niet openen, $!" ;	# Open de output file voor relaties
print OUTPUT "# Lijst met leveranciers:\n" ;
foreach my $el ( $dom->findnodes ( $node ) )				# Zoek eerst de crediteuren
{
  my $dc   = $el->findvalue ( './relationshipID' ) ;
  next if ( $dc ne 'Cred' ) ;						# Alleen crediteuren
  my $name = $el->findvalue ( './custSupName' ) ;
  my $id   = $el->findvalue ( './custSupID' ) ;
  $id = 'L' . substr ( $id, 1 ) ;					# "L" geeft aan: leverancier
  my $land = $el->findvalue ( './streetAddress/country' ) ;		# Land vestiging
  my $icode = $I_HOOG ;							# Neem aan: inkoop hoog tarief
  if ( $land ne 'NL' )							# Maar als het buitenland is...
  {
    $icode = "$I_EU" ;							# dan is tarief onzeker, neem aan EU
  }
  $name =~ tr/$/&/ ;							# Ampersand terugzetten
  $name = decode_entities ( $name ) ;					# Vervang door speciale tekens
  my $line = sprintf ( "relatie --dagboek=Inkopen  %-6s %-70s   %d\n",
                       $id, qq("$name"), $icode ) ;			# Default grootboek
  $leveran{$name} = $id ;						# Zet in tabel
  print OUTPUT $line ;
}
close ( OUTPUT ) ;
print "Leveranciers voor relaties.eb staan nu klaar in $reltmp\n" ;

open ( OUTPUT, ">$tmpfil" ) or die "Kan $tmpfil niet openen, $!" ;	# Open temporary file vooor klanten
print OUTPUT "# Lijst met klanten:\n" ;
foreach my $el ( $dom->findnodes ( $node ) )				# Zoek nu de debiteuren
{
  my $dc   = $el->findvalue ( './relationshipID' ) ;
  next if ( $dc ne 'Deb' ) ; 						# Alleen debiteuren
  my $id   = $el->findvalue ( './custSupID' ) ;
  my $name = $el->findvalue ( './custSupName' ) ;
  $name =~ tr/$/&/ ;							# Ampersand terugzetten
  $name = decode_entities ( $name ) ;					# Vervang door speciale tekens
  $id = sprintf ( "K%04d", substr ( $id, 1 ) + 0 ) ;			# "K" geeft aan: klanten
  my $land = $el->findvalue ( './streetAddress/country' ) ;
  my $tax  = $el->findvalue ( './taxRegIdent' ) ;
  my $dfgr = $D_HOOG ;							# Default grootboek
  if ( ( $land ne 'NL' ) && $tax  )					# Buitenlander met BTW nummer?
  {
    $dfgr = $D_EU ;							# Ja, gebruik EU regel
  }
  my $line = sprintf ( "relatie --dagboek=Verkopen %-6s %-70s   %d\n",
                       $id, qq("$name"), $dfgr ) ;
  $klanten{$name} = $id ;						# Zet in tabel
  print OUTPUT $line ;
}
close ( OUTPUT ) ;
system ( "sort $tmpfil >> $reltmp" ) ;					# Sorteer debiteuren naar outputfile
unlink $tmpfil ;
print "Klanten voor relaties.eb staan nu klaar in $reltmp\n" ;

$node = '/auditfile/company/generalLedger/ledgerAccount' ;		# Zoek de grootboekrekeningen
if ( 0 )								# Verander naar 1 indien interessant
{
  print "\n# De volgende grootboekrekeningen worden gebruikt:\n" ;
  foreach my $el ( $dom->findnodes ( $node ) )				# Zoek in de ledgerAccounts
  {
    my $id  = $el->findvalue ( './accID'   ) ;
    $id = sprintf ( "%04d", $id + 0 ) ;					# Formatteer
    my $tp  = $el->findvalue ( './accTp'   ) ;
    my $dsc = $el->findvalue ( './accDesc' ) ;
    print "$id $tp $dsc\n" ;
  }
  print "\n" ;
}

# Maak een lijst met alle factuurnummers in het vorige jaar
$sql = "SELECT g.fldFactuurnummer, g.fldFactuurNummerID, " .
              "f.fldDatum " .
         "FROM tblVerkoopOrder o,  tblVerkoopOrderRegel r, tblRelatie k, " .
              "tblVerkoopFactuur f, tblFactuurNummer g " .
        "WHERE r.fldVerkoopOrderID = o.fldVerkooporderID " .
          "AND k.fldRelatieID = o.fldRelatieID " .
          "AND f.fldVerkoopFactuurID = o.fldVerkoopFactuurID " .
          "AND g.fldFactuurNummerID = f.fldFactuurNummerID " .
        "ORDER BY g.fldFactuurnummer" ;
$sth = $dbh->prepare ( $sql ) ;
$rv = $sth->execute() or die $DBI::errstr ;
if ( $rv < 0 )
{
  print $DBI::errstr ;
}
while ( @row = $sth->fetchrow_array() )					# Haal factuurnummer op uit DB
{
  my $y = substr ( $row[2], 0, 4 ) ;					# Haal jaartal op
  if ( $y == $ofyear )							# Factuur vasn vorig jaar?
  {
    if ( ! $open_fac{$row[0]} )						# Staat deze er al in?
    {
      $open_fac{$row[0]}  = $row[1] ;					# Nee, zet in de lijst
    }
  }
}
# Nu kijken welke van deze facturen al zijn betaald
$sql = "SELECT MAX(p.flddatum), r.fldfactuurnummerid, r.fldgrootboekid, " .
              "SUM(r.flddebet)-SUM(r.fldcredit) " .
         "FROM tblJournaalpost p,tblJournaalpostregel r " .
        "WHERE p.fldjournaalpostid=r.fldjournaalpostid " .
          "AND r.fldgrootboekid = $gbid_deb " .
          "AND p.flddatum < '$fiscalyear-01-01' " .
          "AND p.flddatum >= '$ofyear-01-01' " .
        "GROUP BY r.fldfactuurnummerid, r.fldgrootboekid" ;
$sth = $dbh->prepare ( $sql ) ;
$rv = $sth->execute() or die $DBI::errstr ;
while ( @row = $sth->fetchrow_array() )					# Haal factuurnummers op uit DB
{
  for my $fn ( keys %open_fac )
  {
    if ( $row[1] && $open_fac{$fn} == $row[1] )
    {
      if ( $row[3] == 0.0 )						# Afgeboekt?
      {
        delete $open_fac{$fn} ;						# Ja, verwijder uit lijst
      }
      else
      {
        $open_fac{$fn} = $row[3] ;					# openstaand bedrag erin
      }
      last ;
    }
  }
}

# Nu de transacties.  Voor een bankafschrift zijn dat 2 of meer regels voor 1 bedrag:
# 1  - voor grootboekrekening van de bank
# 2  - voor grootboekrekening inkopen of verkopen
# 3+ - eventueel betalingsverschil of verzameling bij bijvoorbeeld een declaratie van
#      betalingen via privé creditcard
# Uit de regels voor de tussenrekening balans wordt de openingsbalans gemaakt.
#
open ( OUTPUT, ">$muttmp" ) or die "Kan $muttmp niet openen, $!" ;
# Eerst de verkoopfacturen uit de database halen
# Vorm de toverspreuk die de juiste boekingsregels ophaalt
$sql = "SELECT o.fldDatum, g.fldFactuurnummer, r.fldbedragnakortingen, " .
       "k.fldRelatiecode, r.fldOmschrijving, k.fldBtwNummer " .
         "FROM tblVerkoopOrder o,  tblVerkoopOrderRegel r, tblRelatie k, " .
              "tblVerkoopFactuur f, tblFactuurNummer g " .
        "WHERE o.fldDatum >= '$fiscalyear-01-01' " .
          "AND o.fldDatum <= '$fiscalyear-12-31' " .
          "AND r.fldVerkoopOrderID = o.fldVerkooporderID " .
          "AND k.fldRelatieID = o.fldRelatieID " .
          "AND f.fldVerkoopFactuurID = o.fldVerkoopFactuurID " .
          "AND g.fldFactuurNummerID = f.fldFactuurNummerID " .
        "ORDER BY g.fldFactuurnummer" ;
$sth = $dbh->prepare ( $sql ) ;
$rv = $sth->execute() or die $DBI::errstr ;
if ( $rv < 0 )
{
  print $DBI::errstr ;
}
my $oldfac = -1 ;                                       		# Voor detectie begin nieuwe factuur
my $totaalbedrag = 0.0 ;						# Totaal bedrag verkoopfacturen
while ( @row = $sth->fetchrow_array())
{
  my $dt = $row[0] ;							# Datum in "YYYY-MM-DD 00:00:00"-vorm
  $dt = substr ( $dt, 0, 10 ) ;						# Strip de tijd eraf
  my $yr = substr ( $dt, 0, 4 ) ;					# Bekijk het jaar
  my $fc = $row[1] ;							# Het factuurnummer
  my $pr = $row[2] ;							# De verkoopprijs ex. BTW
  my $prs = sprintf ( "%8.2f", $pr ) ;					# Bedrag netejes formatten
  $prs =~ tr/./,/ ;							# We gebruiken decimale komma
  my $kl = sprintf ( "K%04d", $row[3] ) ;				# Klantnummer in de "K1234"-vorm
  my $ds = $row[4] ;							# Regel omschrijving
  if ( ! $ds )								# Omschrijving is soms leeg
  {
    $ds = "?"x20 ;							# Vraagtekens van maken
  }
  $ds = sprintf ( "%-96s", "\"$ds\"" ) ;				# Regel omschrijving formatten
  my $btw = $row[5] ;							# BTW nummer (buiten NL, maar binnen EU)
  my $gbr = $D_HOOG ;							# Neem aan: Diensten hoog tarief
  if ( $btw )								# Is er een BTW nummer bij deze relatie?
  {
    $gbr = $D_EU ;							# Ja, dan ander grootboeknummer
  }    
  if ( ! $ds )								# Omschrijving is soms leeg
  {
    $ds = "?" ;
  }
  if ( $fc != $oldfac )							# Begin van een nieuwe factuur?
  {
    $oldfac = $fc ;							# Ja, onthoud huidige regel
    # Begin van factuur, maak kopregel met factuurnr, datum en klantcode
    printf OUTPUT "\nverkopen:%s %s \"Factuur internet diensten\" %s",
                  $fc, $dt, $kl ;
  }
  # Nu de vervolgregel
  print OUTPUT " \\\n" ;						# Vervolgregel teken en newline
  printf OUTPUT "     %s %s %d",					# Format Omschrijving, datum, grootboek
                $ds, $prs, $gbr ;
  $totaalbedrag += $pr ;						# Tel het totaal
}
print OUTPUT "\n\n" ;							# Afsluitende newlines
printf "Totaal bedrag verkoop boekingen is %10.2f\n",			# Toon totaalbedrag
       $totaalbedrag ;

# Nu de inkopen zoeken en in mutaties.eb.tmp zetten.

$sql = "SELECT j.flddatum, j.fldBoekstuk as bk," .
              "r.fldfactuurnummerid, r.flddebet, r.fldcredit," .
              "r.fldomschrijving, l.fldrelatiecode," .
              "l.fldlandid " .
         "FROM tblJournaalpost j, tblJournaalpostRegel r, tblGrootboek g, " .
              "tblFactuurNummer f, tblRelatie l " .
        "WHERE j.fldjournaalpostid = r.fldjournaalpostid " .
          "AND g.fldgrootboekid = r.fldgrootboekid " .
          "AND r.fldfactuurnummerid = f.fldfactuurnummerid " .
          "AND l.fldRelatieID = f.fldRelatieID " .
          "AND g.fldnummer = $CREDITEUREN " .
          "AND r.fldcredit <> 0.0" .
          "AND j.fldBoekstuk LIKE '_______' " .
          "AND j.flddatum >= '$fiscalyear-01-01' " .
          "AND j.flddatum <= '$fiscalyear-12-31' " .
        "ORDER BY bk" ;
$sth = $dbh->prepare ( $sql ) ;
$rv = $sth->execute() or die $DBI::errstr ;
if ( $rv < 0 )
{
  print $DBI::errstr ;
}
$totaalbedrag = 0.0 ;							# Totaal bedrag inkoopfacturen
while ( @row = $sth->fetchrow_array())
{
  my $dt = $row[0] ;							# Datum in "YYYY-MM-DD 00:00:00"-vorm
  $dt = substr ( $dt, 0, 10 ) ;						# Strip de tijd eraf
  my $bkst = $row[1] ;							# Boekstuk
  my $oms  = sprintf ( "%-60s", "\"$row[5]\"" ) ;			# Regel omschrijving formatten
  my $levid = sprintf ( "L%03d", $row[6] ) ;				# Leveranciercode "Lxxx"
  my $bdr = sprintf ( "%9.2f", $row[4] ) ;				# Bedrag geformat
  my $land = $row[7] ;							# Landcode
  $bdr =~ tr/./,/ ;							# Decimale komma
  my $line = sprintf ( "inkopen:%s %s %s %s %s %s\n",
                       $bkst, $dt, $oms, $levid, "\"\"", $bdr ) ;
  print OUTPUT $line ;
  $totaalbedrag += $row[4] ;						# Tel het totaal
}
print OUTPUT "\n\n" ;							# Afsluitende newlines
printf "Totaal bedrag inkoop boekingen is %10.2f\n",			# Toon totaalbedrag
       $totaalbedrag ;

$node = '/auditfile/company/transactions/journal' ;			# Zoek de transacties

my $obalans = "" ;							# Hier komt openingsbalans
foreach my $el ( $dom->findnodes ( $node ) )				# Zoek in de ledgerAccounts
{
  my $id  = $el->findvalue ( './jrnID'  ) ;				# Grootboeknummer
  my $dbn = $el->findvalue ( './desc'   ) ;				# Naam van het dagboek
  my $dsc = $dbn ;							# Bewaar oorspr. naam
  my $db  = $dagboek{$id} ;						# Bepaal dagboek
  if ( $db )								# Dagboek naam omzetten?
  {
    $dbn  = $db ;							# Nieuwe naam van het dagboek
  }
  next if ( $id == $DEBITEUREN ) ;					# Deze transacties overslaan
  next if ( $id == $CREDITEUREN ) ;					# Deze transacties overslaan
  foreach my $elx ( $el->findnodes ( './transaction' ) )
  {
    my $newpost = 1 ;							# Begin nieuwe post
    print OUTPUT "\n" ;
    my $nr   = $elx->findvalue ( './nr'     ) ;				# Transactienummer
    my $dt   = $elx->findvalue ( './trDt'   ) ;				# Transactiedatum
    my $amn  = $elx->findvalue ( './amnt'   ) ;				# Bedrag
    my $dsct = $elx->findvalue ( './desc'   ) ;				# Omschrijving
    my $tp   = $elx->findvalue ( './amntTp' ) ;				# D of C
    foreach my $elt ( $elx->findnodes ( './trLine' ) )
    {
      my $acid = $elt->findvalue ( './accID'     ) ;			# Grootboeknummer boeking
      next if ( $acid == $id ) ;					# Skip niet relevante regel
      my $dr   = $elt->findvalue ( './docRef'    ) ;			# Factuurnummer
      my $amnt = $elt->findvalue ( './amnt'      ) ;			# Bedrag
      my $dc   = $elt->findvalue ( './amntTp'    ) ;			# D of C
      my $desc = $elt->findvalue ( './desc'      ) ;			# Omschrijving
      my $rl   = $elt->findvalue ( './custSupID' ) ;			# Relatie vooralsnog onbekend
      if ( $rl )							# Gedefinieerd?
      {									# Ja, is bijvoorbeeld 'c123' of 'd45'
        if ( substr ( $rl, 0, 1 ) eq 'd' )				# Is het een klant?
        {
          $rl = sprintf ( "K%04d", substr ( $rl, 1 ) ) ;		# Ja, maak er een klantcode van
        }
        else
        {
          $rl = sprintf ( "L%03d", substr ( $rl, 1 ) ) ;		# Nee, maak er een leveranciercode van
        }
      }
      else
      {
        $rl = "Onbek." ;
      } 
      $desc =~ tr/$/&/ ;						# Ampersand terugzetten
      #$desc = decode_entities ( $desc ) ;				# Vervang door speciale tekens
      my $bsrt = 'std' ;						# Boekingsoort crd/deb/std
      my $vi = 'Verkopen:' ;						# Neem aan: betaling factuur
      my $tgr = '' ;							# Grootboeknr te boeken
      if ( $id == $BBALNS )						# Beginbalans?
      {
      }
      elsif ( $acid == $DEBITEUREN )					# Debiteur?
      {
        $bsrt = 'deb' ;							# Soort boeking
        $dr = substr ( $dr, 0, 6 ) ;					# Factuurnummer zonder datum
        #if ( $dc eq 'C' )						# Eventueel negatief maken
        if ( $dc eq 'D' )						# Eventueel negatief maken
        {
          $amnt = -$amnt ;
        }
      }
      elsif ( $acid == $CREDITEUREN )					# Crediteur?
      {
        $bsrt = 'crd' ;							# Soort boeking
        #$dr = substr ( $dr, 0, 6 ) ;					# Factuurnummer zonder datum
        #$dr = $nr ;							# Factuurnummer wordt volgnummer
        $vi = $rl ;							# Betaling aan leverancier
        $dr = "" ;							# Geen boekstuk bekend
        if ( $dc eq 'D' )						# Postief of negatief?
        {
          $amnt = -$amnt ;						# Staat 'D' en positief in XAF
        }
      }
      else
      {
        $vi = "\"$desc\"" ;						# Hou omschrijving
        $tgr = $acid ;							# Vermeld grootboeknr
        if ( $dc eq 'D' )						# Algemene kosten?
        {
          $amnt = -$amnt ;						# Ja, dus negatief
          $dr = '' ;							# Geen factuurnr
        }
      }
      my $bedrag = sprintf ( "%9.2f", $amnt ) ;				# Format bedrag
      $bedrag =~ tr/./,/ ;						# Decimale komma
      my $s = sprintf ( "%5d %04d %s %s %s   %-8s  %-5s  \"%s\"\n",
                        $nr, $acid, $dt, $bedrag, $dc, $dr, $rl, $desc ) ;
      if ( $id == $BBALNS )						# Beginbalans?
      {
        if ( $dc eq 'D' )						# Ja, debet?
        {
          $balttl_D += $amnt ;						# Totaal debet bijhouden
        }
        else
        {
          $balttl_C += $amnt ;						# Totaal credit bijhouden
        }
        if ( $acid == $CREDITEUREN )					# Crediteur?
        {
	  $dc = 'B' ;							# Ja, forceer rechts
          $amnt = -$amnt ;						# En negatief
        }
        #print "$dc $bedrag $id $acid\n" ;
        if ( $dc eq 'D' )						# Ja, debet?
        {
          $bedrag = sprintf ( "%9.2f         ", $amnt ) ;		# Plaats links
        }
        else
        {
          $bedrag = sprintf ( "         %9.2f", $amnt ) ;		# Plaats rechts
        }
        $bedrag =~ tr/./,/ ;						# Decimale komma
        my $boms = $reknrs{$acid} ;					# Pak omschrijving
        my $line = sprintf( "adm_balans %04d %s   # %s\n",
                            $acid, $bedrag, $boms ) ;
        $obalans .= $line ;						# Voeg toe aan string
        next ;
      }
      if ( $newpost )
      {
        $newpost = 0 ;
        print OUTPUT "\n$dbn:$nr $dt \"$desc\"  " ;
      }
      if ( $dr ~~ %open_fac )						# Betaling voor openstaande factuur?
      {
        $dr = "$ofyear:$dr" ;						# Ja, jaar in de regel
      }
      printf OUTPUT "\\\n\t$bsrt $dt $vi$dr $bedrag $tgr"  ;
    }
  } 
}
print OUTPUT "\n" ;
close ( OUTPUT ) ;
print "Mutaties staan nu klaar in $muttmp\n" ;

open ( OUTPUT, ">$opntmp" ) or die "Kan $opntmp niet openen, $!" ;	# Schrijf naar opening.eb.tmp
my $bedrag = sprintf ( "%10.2f", $balttl_D ) ;				# Balans totaal
$bedrag =~ tr/./,/ ;							# Decimale komma
print OUTPUT "adm_balanstotaal        $bedrag\n#\n", $obalans ;		# Totaal en balansposten
print OUTPUT "#\n" ;
print OUTPUT "# Openstaande verkoopfacturen van $ofyear:\n" ;		# Schrijf de openstaande facturen
$sql = "SELECT r.fldrelatiecode, r.fldNaam, r.fldContactpersoon " .	# Vorm query voor relatie/factuur
         "FROM tblFactuurNummer f, tblRelatie r " .
        "WHERE f.fldrelatieid = r.fldrelatieid  " .
          "AND f.fldfactuurnummer = ?" ;
$sth = $dbh->prepare ( $sql ) ;
for my $fn ( sort keys %open_fac )
{
  $rv = $sth->execute($fn) or die $DBI::errstr ;
  if ( @row = $sth->fetchrow_array() )					# Haal relatie op uit DB
  {
    my $nm = $row[1] ;							# Pak bedrijfsnaam
    if ( ! $nm )							# Is het een bedrijf?
    {
      $nm = $row[2] ;							# Nee, neem contactpersoon
    }
    $nm = decode_entities ( $nm ) ;					# Vervang door speciale tekens
    my $bedrag = sprintf ( "%8.2f", $open_fac{$fn} ) ;			# Format bedrag
    $bedrag =~ tr/./,/ ;						# Decimale komma
    my $line = sprintf( "adm_relatie Verkopen:%d:%s %d-12-31 K%04d \"%-20.20s\" %s\n",
                        $ofyear, $fn, $ofyear, $row[0], $nm, $bedrag) ;
    print OUTPUT $line ;
  }
}
close ( OUTPUT ) ;
print "Openingsbalans en openstaande facturen staan nu klaar in $opntmp\n" ;


