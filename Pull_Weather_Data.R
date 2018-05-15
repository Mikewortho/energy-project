library("rnoaa")

noaakey = "HEfWGerRwNAttptRrXwtdJnOIUKStql"
options("noaakey" = Sys.getenv("noaakey"))

stations <- isd_stations()
head(stations)


# ALABAMA  
AL1 <- isd(usaf = "720265", wban = "63833", year = 2015)
AL2 <- isd(usaf = "720265", wban = "63833", year = 2016 )
AL3 <- isd(usaf = "720265", wban = "63833", year = 2017 )
AL4 <- isd(usaf = "720265", wban = "63833", year = 2018 )

NEWAL1 <- AL1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
NEWAL2 <- AL2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
NEWAL3 <- AL3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
NEWAL4 <- AL4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

ALABAMA = rbind(as.matrix(NEWAL1),as.matrix(NEWAL2),as.matrix(NEWAL3),as.matrix(NEWAL4))
ALABAMA <- cbind(ALABAMA,State='ALABAMA');
head(ALABAMA)
tail(ALABAMA)

# ALASKA
AK1 <- isd(usaf = "701337", wban = "00103", year = 2015)
AK2 <- isd(usaf = "701337", wban = "00103", year = 2016 )
AK3 <- isd(usaf = "701337", wban = "00103", year = 2017 )
AK4 <- isd(usaf = "701337", wban = "00103", year = 2018 )

newAK1 <- AK1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAK2 <- AK2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAK3 <- AK3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAK4 <- AK4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

ALASKA = rbind(as.matrix(newAK1),as.matrix(newAK2),as.matrix(newAK3),as.matrix(newAK4))
ALASKA <- cbind(ALASKA,State='ALASKA');
head(ALASKA)
tail(ALASKA)

# ARIZONA
AZ1 <- isd(usaf = "699604", wban = "03145", year = 2015)
AZ2 <- isd(usaf = "699604", wban = "03145", year = 2016 )
AZ3 <- isd(usaf = "699604", wban = "03145", year = 2017 )
AZ4 <- isd(usaf = "699604", wban = "03145", year = 2018 )

newAZ1 <- AZ1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAZ2 <- AZ2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAZ3 <- AZ3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAZ4 <- AZ4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

ARIZONA = rbind(as.matrix(newAZ1),as.matrix(newAZ2),as.matrix(newAZ3),as.matrix(newAZ4))
ARIZONA <- cbind(ARIZONA,State='ARIZONA');
head(ARIZONA)
tail(ARIZONA)

# ARKANSAS
AR1 <- isd(usaf = "720172", wban = "53996", year = 2015)
AR2 <- isd(usaf = "720172", wban = "53996", year = 2016 )
AR3 <- isd(usaf = "720172", wban = "53996", year = 2017 )
AR4 <- isd(usaf = "720172", wban = "53996", year = 2018 )

newAR1 <- AR1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAR2 <- AR2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAR3 <- AR3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newAR4 <- AR4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

ARKANSAS = rbind(as.matrix(newAR1),as.matrix(newAR2),as.matrix(newAR3),as.matrix(newAR4))
ARKANSAS <- cbind(ARKANSAS,State='ARKANSAS');
head(ARKANSAS)
tail(ARKANSAS)

# CALIFORNIA
CA1 <- isd(usaf = "720193", wban = "99999", year = 2015)
CA2 <- isd(usaf = "720193", wban = "99999", year = 2016 )
CA3 <- isd(usaf = "720193", wban = "99999", year = 2017 )
CA4 <- isd(usaf = "720193", wban = "99999", year = 2018 )

newCA1 <- CA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCA2 <- CA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCA3 <- CA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCA4 <- CA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

CALIFORNIA = rbind(as.matrix(newCA1),as.matrix(newCA2),as.matrix(newCA3),as.matrix(newCA4))
CALIFORNIA <- cbind(CALIFORNIA,State='CALIFORNIA');
head(CALIFORNIA)
tail(CALIFORNIA)

# COLORADO
CO1 <- isd(usaf = "720262", wban = "94076", year = 2015)
CO2 <- isd(usaf = "720262", wban = "94076", year = 2016 )
CO3 <- isd(usaf = "720262", wban = "94076", year = 2017 )
CO4 <- isd(usaf = "720262", wban = "94076", year = 2018 )

newCO1 <- CO1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCO2 <- CO2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCO3 <- CO3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCO4 <- CO4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

COLORADO = rbind(as.matrix(newCO1),as.matrix(newCO2),as.matrix(newCO3),as.matrix(newCO4))
COLORADO <- cbind(COLORADO,State='COLORADO');
head(COLORADO)
tail(COLORADO)

# CONNECTICUT
CT1 <- isd(usaf = "725027", wban = "54788", year = 2015)
CT2 <- isd(usaf = "725027", wban = "54788", year = 2016 )
CT3 <- isd(usaf = "725027", wban = "54788", year = 2017 )
CT4 <- isd(usaf = "725027", wban = "54788", year = 2018 )

newCT1 <- CT1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCT2 <- CT2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCT3 <- CT3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newCT4 <- CT4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

CONNECTICUT = rbind(as.matrix(newCT1),as.matrix(newCT2),as.matrix(newCT3),as.matrix(newCT4))
CONNECTICUT <- cbind(CONNECTICUT,State='CONNECTICUT');
head(CONNECTICUT)
tail(CONNECTICUT)

# DELAWARE
DE1 <- isd(usaf = "997281", wban = "99999", year = 2015)
DE2 <- isd(usaf = "997281", wban = "99999", year = 2016 )
DE3 <- isd(usaf = "997281", wban = "99999", year = 2017 )
DE4 <- isd(usaf = "997281", wban = "99999", year = 2018 )

newDE1 <- DE1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newDE2 <- DE2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newDE3 <- DE3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newDE4 <- DE4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

DELAWARE = rbind(as.matrix(newDE1),as.matrix(newDE2),as.matrix(newDE3),as.matrix(newDE4))
DELAWARE <- cbind(DELAWARE,State='DELAWARE');
head(DELAWARE)
tail(DELAWARE)

# FLORIDA
FL1 <- isd(usaf = "994951", wban = "99999", year = 2015)
FL2 <- isd(usaf = "994951", wban = "99999", year = 2016 )
FL3 <- isd(usaf = "994951", wban = "99999", year = 2017 )
FL4 <- isd(usaf = "994951", wban = "99999", year = 2018 )

newFL1 <- FL1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newFL2 <- FL2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newFL3 <- FL3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newFL4 <- FL4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

FLORIDA = rbind(as.matrix(newFL1),as.matrix(newFL2),as.matrix(newFL3),as.matrix(newFL4))
FLORIDA <- cbind(FLORIDA,State='FLORIDA');
head(FLORIDA)
tail(FLORIDA)

# GEORGIA
GA1 <- isd(usaf = "747812", wban = "63813", year = 2015)
GA2 <- isd(usaf = "747812", wban = "63813", year = 2016 )
GA3 <- isd(usaf = "747812", wban = "63813", year = 2017 )
GA4 <- isd(usaf = "747812", wban = "63813", year = 2018 )

newGA1 <- GA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newGA2 <- GA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newGA3 <- GA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newGA4 <- GA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

GEORGIA = rbind(as.matrix(newGA1),as.matrix(newGA2),as.matrix(newGA3),as.matrix(newGA4))
GEORGIA <- cbind(GEORGIA,State='GEORGIA');
head(GEORGIA)
tail(GEORGIA)

# HAWAII
HI1 <- isd(usaf = "911700", wban = "22508", year = 2015)
HI2 <- isd(usaf = "911700", wban = "22508", year = 2016 )
HI3 <- isd(usaf = "911700", wban = "22508", year = 2017 )
HI4 <- isd(usaf = "911700", wban = "22508", year = 2018 )

newHI1 <- HI1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newHI2 <- HI2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newHI3 <- HI3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newHI4 <- HI4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

HAWAII = rbind(as.matrix(newHI1),as.matrix(newHI2),as.matrix(newHI3),as.matrix(newHI4))
HAWAII <- cbind(HAWAII,State='HAWAII');
head(HAWAII)
tail(HAWAII)

# IDAHO
ID1 <- isd(usaf = "727834", wban = "24136", year = 2015)
ID2 <- isd(usaf = "727834", wban = "24136", year = 2016 )
ID3 <- isd(usaf = "727834", wban = "24136", year = 2017 )
ID4 <- isd(usaf = "727834", wban = "24136", year = 2018 )

newID1 <- ID1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newID2 <- ID2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newID3 <- ID3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newID4 <- ID4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

IDAHO = rbind(as.matrix(newID1),as.matrix(newID2),as.matrix(newID3),as.matrix(newID4))
IDAHO <- cbind(IDAHO,State='IDAHO');
head(IDAHO)
tail(IDAHO)

# ILLINOIS
IL1 <- isd(usaf = "744652", wban = "53897", year = 2015)
IL2 <- isd(usaf = "744652", wban = "53897", year = 2016 )
IL3 <- isd(usaf = "744652", wban = "53897", year = 2017 )
IL4 <- isd(usaf = "744652", wban = "53897", year = 2018 )

newIL1 <- IL1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIL2 <- IL2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIL3 <- IL3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIL4 <- IL4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

ILLINOIS = rbind(as.matrix(newIL1),as.matrix(newIL2),as.matrix(newIL3),as.matrix(newIL4))
ILLINOIS <- cbind(ILLINOIS,State='ILLINOIS');
head(ILLINOIS)
tail(ILLINOIS)

# INDIANA
IN1 <- isd(usaf = "744214", wban = "99999", year = 2015)
IN2 <- isd(usaf = "744214", wban = "99999", year = 2016 )
IN3 <- isd(usaf = "744214", wban = "99999", year = 2017 )
IN4 <- isd(usaf = "744214", wban = "99999", year = 2018 )

newIN1 <- IN1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIN2 <- IN2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIN3 <- IN3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIN4 <- IN4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

INDIANA = rbind(as.matrix(newIN1),as.matrix(newIN2),as.matrix(newIN3),as.matrix(newIN4))
INDIANA <- cbind(INDIANA,State='INDIANA');
head(INDIANA)
tail(INDIANA)

# IOWA
IA1 <- isd(usaf = "725473", wban = "94979", year = 2015)
IA2 <- isd(usaf = "725473", wban = "94979", year = 2016 )
IA3 <- isd(usaf = "725473", wban = "94979", year = 2017 )
IA4 <- isd(usaf = "725473", wban = "94979", year = 2018 )

newIA1 <- IA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIA2 <- IA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIA3 <- IA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newIA4 <- IA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

IOWA = rbind(as.matrix(newIA1),as.matrix(newIA2),as.matrix(newIA3),as.matrix(newIA4))
IOWA <- cbind(IOWA,State='IOWA');
head(IOWA)
tail(IOWA)

# KANSAS
KS1 <- isd(usaf = "745430", wban = "93978", year = 2015)
KS2 <- isd(usaf = "745430", wban = "93978", year = 2016 )
KS3 <- isd(usaf = "745430", wban = "93978", year = 2017 )
KS4 <- isd(usaf = "745430", wban = "93978", year = 2018 )

newKS1 <- KS1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKS2 <- KS2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKS3 <- KS3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKS4 <- KS4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

KANSAS = rbind(as.matrix(newKS1),as.matrix(newKS2),as.matrix(newKS3),as.matrix(newKS4))
KANSAS <- cbind(KANSAS,State='KANSAS');
head(KANSAS)
tail(KANSAS)

# KENTUCKY
KY1 <- isd(usaf = "724350", wban = "03816", year = 2015)
KY2 <- isd(usaf = "724350", wban = "03816", year = 2016 )
KY3 <- isd(usaf = "724350", wban = "03816", year = 2017 )
KY4 <- isd(usaf = "724350", wban = "03816", year = 2018 )

newKY1 <- KY1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKY2 <- KY2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKY3 <- KY3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newKY4 <- KY4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

KENTUCKY = rbind(as.matrix(newKY1),as.matrix(newKY2),as.matrix(newKY3),as.matrix(newKY4))
KENTUCKY <- cbind(KENTUCKY,State='KENTUCKY');
head(KENTUCKY)
tail(KENTUCKY)

# LOUISIANA
LA1 <- isd(usaf = "722821", wban = "53988", year = 2015)
LA2 <- isd(usaf = "722821", wban = "53988", year = 2016 )
LA3 <- isd(usaf = "722821", wban = "53988", year = 2017 )
LA4 <- isd(usaf = "722821", wban = "53988", year = 2018 )

newLA1 <- LA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newLA2 <- LA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newLA3 <- LA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newLA4 <- LA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

LOUISIANA = rbind(as.matrix(newLA1),as.matrix(newLA2),as.matrix(newLA3),as.matrix(newLA4))
LOUISIANA <- cbind(LOUISIANA,State='LOUISIANA');
head(LOUISIANA)
tail(LOUISIANA)

# MAINE
ME1 <- isd(usaf = "994060", wban = "99999", year = 2015)
ME2 <- isd(usaf = "994060", wban = "99999", year = 2016 )
ME3 <- isd(usaf = "994060", wban = "99999", year = 2017 )
ME4 <- isd(usaf = "994060", wban = "99999", year = 2018 )

newME1 <- ME1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newME2 <- ME2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newME3 <- ME3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newME4 <- ME4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MAINE = rbind(as.matrix(newME1),as.matrix(newME2),as.matrix(newME3),as.matrix(newME4))
MAINE <- cbind(MAINE,State='MAINE');
head(MAINE)
tail(MAINE)

# MARYLAND
MD1 <- isd(usaf = "997296", wban = "99999", year = 2015)
MD2 <- isd(usaf = "997296", wban = "99999", year = 2016 )
MD3 <- isd(usaf = "997296", wban = "99999", year = 2017 )
MD4 <- isd(usaf = "997296", wban = "99999", year = 2018 )

newMD1 <- MD1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMD2 <- MD2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMD3 <- MD3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMD4 <- MD4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MARYLAND = rbind(as.matrix(newMD1),as.matrix(newMD2),as.matrix(newMD3),as.matrix(newMD4))
MARYLAND <- cbind(MARYLAND,State='MARYLAND');
head(MARYLAND)
tail(MARYLAND)

# MASSACHUSETTS
MA1 <- isd(usaf = "997279", wban = "99999", year = 2015)
MA2 <- isd(usaf = "997279", wban = "99999", year = 2016 )
MA3 <- isd(usaf = "997279", wban = "99999", year = 2017 )
MA4 <- isd(usaf = "997279", wban = "99999", year = 2018 )

newMA1 <- MA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMA2 <- MA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMA3 <- MA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMA4 <- MA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MASSACHUSETTS = rbind(as.matrix(newMA1),as.matrix(newMA2),as.matrix(newMA3),as.matrix(newMA4))
MASSACHUSETTS <- cbind(MASSACHUSETTS,State='MASSACHUSETTS');
head(MASSACHUSETTS)
tail(MASSACHUSETTS)

# MICHIGAN
MI1 <- isd(usaf = "997079", wban = "99999", year = 2015)
MI2 <- isd(usaf = "997079", wban = "99999", year = 2016 )
MI3 <- isd(usaf = "997079", wban = "99999", year = 2017 )
MI4 <- isd(usaf = "997079", wban = "99999", year = 2018 )

newMI1 <- MI1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMI2 <- MI2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMI3 <- MI3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMI4 <- MI4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MICHIGAN = rbind(as.matrix(newMI1),as.matrix(newMI2),as.matrix(newMI3),as.matrix(newMI4))
MICHIGAN <- cbind(MICHIGAN,State='MICHIGAN');
head(MICHIGAN)
tail(MICHIGAN)

# MINNESOTA
MN1 <- isd(usaf = "997737", wban = "99999", year = 2015)
MN2 <- isd(usaf = "997737", wban = "99999", year = 2016 )
MN3 <- isd(usaf = "997737", wban = "99999", year = 2017 )
MN4 <- isd(usaf = "997737", wban = "99999", year = 2018 )

newMN1 <- MN1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMN2 <- MN2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMN3 <- MN3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMN4 <- MN4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MINNESOTA = rbind(as.matrix(newMN1),as.matrix(newMN2),as.matrix(newMN3),as.matrix(newMN4))
MINNESOTA <- cbind(MINNESOTA,State='MINNESOTA');
head(MINNESOTA)
tail(MINNESOTA)

# MISSISSIPPI
MS1 <- isd(usaf = "998171", wban = "99999", year = 2015)
MS2 <- isd(usaf = "998171", wban = "99999", year = 2016 )
MS3 <- isd(usaf = "998171", wban = "99999", year = 2017 )
MS4 <- isd(usaf = "998171", wban = "99999", year = 2018 )

newMS1 <- MS1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMS2 <- MS2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMS3 <- MS3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMS4 <- MS4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MISSISSIPPI = rbind(as.matrix(newMS1),as.matrix(newMS2),as.matrix(newMS3),as.matrix(newMS4))
MISSISSIPPI <- cbind(MISSISSIPPI,State='MISSISSIPPI');
head(MISSISSIPPI)
tail(MISSISSIPPI)

# MISSOURI
MO1 <- isd(usaf = "999999", wban = "00451", year = 2015)
MO2 <- isd(usaf = "999999", wban = "00451", year = 2016 )
MO3 <- isd(usaf = "999999", wban = "00451", year = 2017 )
MO4 <- isd(usaf = "999999", wban = "00451", year = 2018 )

newMO1 <- MO1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMO2 <- MO2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMO3 <- MO3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMO4 <- MO4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MISSOURI = rbind(as.matrix(newMO1),as.matrix(newMO2),as.matrix(newMO3),as.matrix(newMO4))
MISSOURI <- cbind(MISSOURI,State='MISSOURI');
head(MISSOURI)
tail(MISSOURI)

# MONTANA
MT1 <- isd(usaf = "720994", wban = "99999", year = 2015)
MT2 <- isd(usaf = "720994", wban = "99999", year = 2016 )
MT3 <- isd(usaf = "720994", wban = "99999", year = 2017 )
MT4 <- isd(usaf = "720994", wban = "99999", year = 2018 )

newMT1 <- MT1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMT2 <- MT2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMT3 <- MT3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newMT4 <- MT4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

MONTANA = rbind(as.matrix(newMT1),as.matrix(newMT2),as.matrix(newMT3),as.matrix(newMT4))
MONTANA <- cbind(MONTANA,State='MONTANA');
head(MONTANA)
tail(MONTANA)

# NEBRASKA
NE1 <- isd(usaf = "722124", wban = "04998", year = 2015)
NE2 <- isd(usaf = "722124", wban = "04998", year = 2016 )
NE3 <- isd(usaf = "722124", wban = "04998", year = 2017 )
NE4 <- isd(usaf = "722124", wban = "04998", year = 2018 )

newNE1 <- NE1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNE2 <- NE2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNE3 <- NE3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNE4 <- NE4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEBRASKA = rbind(as.matrix(newNE1),as.matrix(newNE2),as.matrix(newNE3),as.matrix(newNE4))
NEBRASKA <- cbind(NEBRASKA,State='NEBRASKA');
head(NEBRASKA)
tail(NEBRASKA)

# NEVADA
NV1 <- isd(usaf = "723860", wban = "23169", year = 2015)
NV2 <- isd(usaf = "723860", wban = "23169", year = 2016 )
NV3 <- isd(usaf = "723860", wban = "23169", year = 2017 )
NV4 <- isd(usaf = "723860", wban = "23169", year = 2018 )

newNV1 <- NV1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNV2 <- NV2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNV3 <- NV3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNV4 <- NV4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEVADA = rbind(as.matrix(newNV1),as.matrix(newNV2),as.matrix(newNV3),as.matrix(newNV4))
NEVADA <- cbind(NEVADA,State='NEVADA');
head(NEVADA)
tail(NEVADA)

# NEWHAMPSHIRE
NH1 <- isd(usaf = "726050", wban = "14745", year = 2015)
NH2 <- isd(usaf = "726050", wban = "14745", year = 2016 )
NH3 <- isd(usaf = "726050", wban = "14745", year = 2017 )
NH4 <- isd(usaf = "726050", wban = "14745", year = 2018 )

newNH1 <- NH1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNH2 <- NH2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNH3 <- NH3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNH4 <- NH4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEWHAMPSHIRE = rbind(as.matrix(newNH1),as.matrix(newNH2),as.matrix(newNH3),as.matrix(newNH4))
NEWHAMPSHIRE <- cbind(NEWHAMPSHIRE,State='NEW HAMPSHIRE');
head(NEWHAMPSHIRE)
tail(NEWHAMPSHIRE)

# NEWJERSEY
NJ1 <- isd(usaf = "725020", wban = "14734", year = 2015)
NJ2 <- isd(usaf = "725020", wban = "14734", year = 2016 )
NJ3 <- isd(usaf = "725020", wban = "14734", year = 2017 )
NJ4 <- isd(usaf = "725020", wban = "14734", year = 2018 )

newNJ1 <- NJ1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNJ2 <- NJ2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNJ3 <- NJ3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNJ4 <- NJ4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEWJERSEY = rbind(as.matrix(newNJ1),as.matrix(newNJ2),as.matrix(newNJ3),as.matrix(newNJ4))
NEWJERSEY <- cbind(NEWJERSEY,State='NEW JERSEY');
head(NEWJERSEY)
tail(NEWJERSEY)

# NEWMEXICO
NM1 <- isd(usaf = "723663", wban = "03012", year = 2015)
NM2 <- isd(usaf = "723663", wban = "03012", year = 2016 )
NM3 <- isd(usaf = "723663", wban = "03012", year = 2017 )
NM4 <- isd(usaf = "723663", wban = "03012", year = 2018 )

newNM1 <- NM1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNM2 <- NM2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNM3 <- NM3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNM4 <- NM4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEWMEXICO = rbind(as.matrix(newNM1),as.matrix(newNM2),as.matrix(newNM3),as.matrix(newNM4))
NEWMEXICO <- cbind(NEWMEXICO,State='NEW MEXICO');
head(NEWMEXICO)
tail(NEWMEXICO)

# NEWYORK
NY1 <- isd(usaf = "725014", wban = "54780", year = 2015)
NY2 <- isd(usaf = "725014", wban = "54780", year = 2016 )
NY3 <- isd(usaf = "725014", wban = "54780", year = 2017 )
NY4 <- isd(usaf = "725014", wban = "54780", year = 2018 )

newNY1 <- NY1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNY2 <- NY2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNY3 <- NY3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNY4 <- NY4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NEWYORK = rbind(as.matrix(newNY1),as.matrix(newNY2),as.matrix(newNY3),as.matrix(newNY4))
NEWYORK <- cbind(NEWYORK,State='NEW YORK');
head(NEWYORK)
tail(NEWYORK)

# NORTHCAROLINA
NC1 <- isd(usaf = "725294", wban = "99999", year = 2015)
NC2 <- isd(usaf = "725294", wban = "99999", year = 2016 )
NC3 <- isd(usaf = "725294", wban = "99999", year = 2017 )
NC4 <- isd(usaf = "725294", wban = "99999", year = 2018 )

newNC1 <- NC1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNC2 <- NC2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNC3 <- NC3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newNC4 <- NC4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NORTHCAROLINA = rbind(as.matrix(newNC1),as.matrix(newNC2),as.matrix(newNC3),as.matrix(newNC4))
NORTHCAROLINA <- cbind(NORTHCAROLINA,State='NORTH CAROLINA');
head(NORTHCAROLINA)
tail(NORTHCAROLINA)

# NORTHDAKOTA
ND1 <- isd(usaf = "727535", wban = "14919", year = 2015)
ND2 <- isd(usaf = "727535", wban = "14919", year = 2016 )
ND3 <- isd(usaf = "727535", wban = "14919", year = 2017 )
ND4 <- isd(usaf = "727535", wban = "14919", year = 2018 )

newND1 <- ND1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newND2 <- ND2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newND3 <- ND3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newND4 <- ND4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

NORTHDAKOTA = rbind(as.matrix(newND1),as.matrix(newND2),as.matrix(newND3),as.matrix(newND4))
NORTHDAKOTA <- cbind(NORTHDAKOTA,State='NORTH DAKOTA');
head(NORTHDAKOTA)
tail(NORTHDAKOTA)

# OHIO
OH1 <- isd(usaf = "725360", wban = "94830", year = 2015)
OH2 <- isd(usaf = "725360", wban = "94830", year = 2016 )
OH3 <- isd(usaf = "725360", wban = "94830", year = 2017 )
OH4 <- isd(usaf = "725360", wban = "94830", year = 2018 )

newOH1 <- OH1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOH2 <- OH2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOH3 <- OH3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOH4 <- OH4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

OHIO = rbind(as.matrix(newOH1),as.matrix(newOH2),as.matrix(newOH3),as.matrix(newOH4))
OHIO <- cbind(OHIO,State='OHIO');
head(OHIO)
tail(OHIO)

# OKLAHOMA
OK1 <- isd(usaf = "723556", wban = "93953", year = 2015)
OK2 <- isd(usaf = "723556", wban = "93953", year = 2016 )
OK3 <- isd(usaf = "723556", wban = "93953", year = 2017 )
OK4 <- isd(usaf = "723556", wban = "93953", year = 2018 )

newOK1 <- OK1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOK2 <- OK2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOK3 <- OK3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOK4 <- OK4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

OKLAHOMA = rbind(as.matrix(newOK1),as.matrix(newOK2),as.matrix(newOK3),as.matrix(newOK4))
OKLAHOMA <- cbind(OKLAHOMA,State='OKLAHOMA');
head(OKLAHOMA)
tail(OKLAHOMA)

# OREGON
OR1 <- isd(usaf = "725895", wban = "94236", year = 2015)
OR2 <- isd(usaf = "725895", wban = "94236", year = 2016 )
OR3 <- isd(usaf = "725895", wban = "94236", year = 2017 )
OR4 <- isd(usaf = "725895", wban = "94236", year = 2018 )

newOR1 <- OR1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOR2 <- OR2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOR3 <- OR3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newOR4 <- OR4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

OREGON = rbind(as.matrix(newOR1),as.matrix(newOR2),as.matrix(newOR3),as.matrix(newOR4))
OREGON <- cbind(OREGON,State='OREGON');
head(OREGON)
tail(OREGON)

# PENNSYLVANIA
PA1 <- isd(usaf = "720324", wban = "64753", year = 2015)
PA2 <- isd(usaf = "720324", wban = "64753", year = 2016 )
PA3 <- isd(usaf = "720324", wban = "64753", year = 2017 )
PA4 <- isd(usaf = "720324", wban = "64753", year = 2018 )

newPA1 <- PA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newPA2 <- PA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newPA3 <- PA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newPA4 <- PA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

PENNSYLVANIA = rbind(as.matrix(newPA1),as.matrix(newPA2),as.matrix(newPA3),as.matrix(newPA4))
PENNSYLVANIA <- cbind(PENNSYLVANIA,State='PENNSYLVANIA');
head(PENNSYLVANIA)
tail(PENNSYLVANIA)

# RHODEISLAND
RI1 <- isd(usaf = "725058", wban = "94793", year = 2015)
RI2 <- isd(usaf = "725058", wban = "94793", year = 2016 )
RI3 <- isd(usaf = "725058", wban = "94793", year = 2017 )
RI4 <- isd(usaf = "725058", wban = "94793", year = 2018 )

newRI1 <- RI1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newRI2 <- RI2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newRI3 <- RI3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newRI4 <- RI4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

RHODEISLAND = rbind(as.matrix(newRI1),as.matrix(newRI2),as.matrix(newRI3),as.matrix(newRI4))
RHODEISLAND <- cbind(RHODEISLAND,State='RHODE ISLAND');
head(RHODEISLAND)
tail(RHODEISLAND)

# SOUTHCAROLINA
SC1 <- isd(usaf = "747910", wban = "13717", year = 2015)
SC2 <- isd(usaf = "747910", wban = "13717", year = 2016 )
SC3 <- isd(usaf = "747910", wban = "13717", year = 2017 )
SC4 <- isd(usaf = "747910", wban = "13717", year = 2018 )

newSC1 <- SC1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSC2 <- SC2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSC3 <- SC3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSC4 <- SC4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

SOUTHCAROLINA = rbind(as.matrix(newSC1),as.matrix(newSC2),as.matrix(newSC3),as.matrix(newSC4))
SOUTHCAROLINA <- cbind(SOUTHCAROLINA,State='SOUTH CAROLINA');
head(SOUTHCAROLINA)
tail(SOUTHCAROLINA)

# SOUTHDAKOTA
SD1 <- isd(usaf = "726685", wban = "94052", year = 2015)
SD2 <- isd(usaf = "726685", wban = "94052", year = 2016 )
SD3 <- isd(usaf = "726685", wban = "94052", year = 2017 )
SD4 <- isd(usaf = "726685", wban = "94052", year = 2018 )

newSD1 <- SD1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSD2 <- SD2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSD3 <- SD3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newSD4 <- SD4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

SOUTHDAKOTA = rbind(as.matrix(newSD1),as.matrix(newSD2),as.matrix(newSD3),as.matrix(newSD4))
SOUTHDAKOTA <- cbind(SOUTHDAKOTA,State='SOUTH DAKOTA');
head(SOUTHDAKOTA)
tail(SOUTHDAKOTA)

# TENNESSEE
TN1 <- isd(usaf = "723340", wban = "13893", year = 2015)
TN2 <- isd(usaf = "723340", wban = "13893", year = 2016 )
TN3 <- isd(usaf = "723340", wban = "13893", year = 2017 )
TN4 <- isd(usaf = "723340", wban = "13893", year = 2018 )

newTN1 <- TN1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTN2 <- TN2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTN3 <- TN3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTN4 <- TN4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

TENNESSEE = rbind(as.matrix(newTN1),as.matrix(newTN2),as.matrix(newTN3),as.matrix(newTN4))
TENNESSEE <- cbind(TENNESSEE,State='TENNESSEE');
head(TENNESSEE)
tail(TENNESSEE)

# TEXAS
TX1 <- isd(usaf = "723510", wban = "13966", year = 2015)
TX2 <- isd(usaf = "723510", wban = "13966", year = 2016 )
TX3 <- isd(usaf = "723510", wban = "13966", year = 2017 )
TX4 <- isd(usaf = "723510", wban = "13966", year = 2018 )

newTX1 <- TX1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTX2 <- TX2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTX3 <- TX3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newTX4 <- TX4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

TEXAS = rbind(as.matrix(newTX1),as.matrix(newTX2),as.matrix(newTX3),as.matrix(newTX4))
TEXAS <- cbind(TEXAS,State='TEXAS');
head(TEXAS)
tail(TEXAS)

# UTAH
UT1 <- isd(usaf = "724720", wban = "99999", year = 2015)
UT2 <- isd(usaf = "724720", wban = "99999", year = 2016 )
UT3 <- isd(usaf = "724720", wban = "99999", year = 2017 )
UT4 <- isd(usaf = "724720", wban = "99999", year = 2018 )

newUT1 <- UT1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newUT2 <- UT2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newUT3 <- UT3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newUT4 <- UT4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

UTAH = rbind(as.matrix(newUT1),as.matrix(newUT2),as.matrix(newUT3),as.matrix(newUT4))
UTAH <- cbind(UTAH,State='UTAH');
head(UTAH)
tail(UTAH)

# VERMONT
VT1 <- isd(usaf = "725165", wban = "94737", year = 2015)
VT2 <- isd(usaf = "725165", wban = "94737", year = 2016 )
VT3 <- isd(usaf = "725165", wban = "94737", year = 2017 )
VT4 <- isd(usaf = "725165", wban = "94737", year = 2018 )

newVT1 <- VT1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVT2 <- VT2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVT3 <- VT3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVT4 <- VT4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

VERMONT = rbind(as.matrix(newVT1),as.matrix(newVT2),as.matrix(newVT3),as.matrix(newVT4))
VERMONT <- cbind(VERMONT,State='VERMONT');
head(VERMONT)
tail(VERMONT)

# VIRGINIA
VA1 <- isd(usaf = "725172", wban = "13762", year = 2015)
VA2 <- isd(usaf = "725172", wban = "13762", year = 2016 )
VA3 <- isd(usaf = "725172", wban = "13762", year = 2017 )
VA4 <- isd(usaf = "725172", wban = "13762", year = 2018 )

newVA1 <- VA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVA2 <- VA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVA3 <- VA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newVA4 <- VA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

VIRGINIA = rbind(as.matrix(newVA1),as.matrix(newVA2),as.matrix(newVA3),as.matrix(newVA4))
VIRGINIA <- cbind(VIRGINIA,State='VIRGINIA');
head(VIRGINIA)
tail(VIRGINIA)

# WASHINGTON
WA1 <- isd(usaf = "727825", wban = "94239", year = 2015)
WA2 <- isd(usaf = "727825", wban = "94239", year = 2016 )
WA3 <- isd(usaf = "727825", wban = "94239", year = 2017 )
WA4 <- isd(usaf = "727825", wban = "94239", year = 2018 )

newWA1 <- WA1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWA2 <- WA2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWA3 <- WA3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWA4 <- WA4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

WASHINGTON = rbind(as.matrix(newWA1),as.matrix(newWA2),as.matrix(newWA3),as.matrix(newWA4))
WASHINGTON <- cbind(WASHINGTON,State='WASHINGTON');
head(WASHINGTON)
tail(WASHINGTON)

# WESTVIRGINIA
WV1 <- isd(usaf = "724175", wban = "03802", year = 2015)
WV2 <- isd(usaf = "724175", wban = "03802", year = 2016 )
WV3 <- isd(usaf = "724175", wban = "03802", year = 2017 )
WV4 <- isd(usaf = "724175", wban = "03802", year = 2018 )

newWV1 <- WV1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWV2 <- WV2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWV3 <- WV3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWV4 <- WV4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

WESTVIRGINIA = rbind(as.matrix(newWV1),as.matrix(newWV2),as.matrix(newWV3),as.matrix(newWV4))
WESTVIRGINIA <- cbind(WESTVIRGINIA,State='WEST VIRGINIA');
head(WESTVIRGINIA)
tail(WESTVIRGINIA)

# WISCONSIN
WI1 <- isd(usaf = "726413", wban = "04875", year = 2015)
WI2 <- isd(usaf = "726413", wban = "04875", year = 2016 )
WI3 <- isd(usaf = "726413", wban = "04875", year = 2017 )
WI4 <- isd(usaf = "726413", wban = "04875", year = 2018 )

newWI1 <- WI1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWI2 <- WI2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWI3 <- WI3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWI4 <- WI4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

WISCONSIN = rbind(as.matrix(newWI1),as.matrix(newWI2),as.matrix(newWI3),as.matrix(newWI4))
WISCONSIN <- cbind(WISCONSIN,State='WISCONSIN');
head(WISCONSIN)
tail(WISCONSIN)

# WYOMING
WY1 <- isd(usaf = "726700", wban = "24045", year = 2015)
WY2 <- isd(usaf = "726700", wban = "24045", year = 2016 )
WY3 <- isd(usaf = "726700", wban = "24045", year = 2017 )
WY4 <- isd(usaf = "726700", wban = "24045", year = 2018 )

newWY1 <- WY1[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWY2 <- WY2[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWY3 <- WY3[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]
newWY4 <- WY4[,c("usaf_station", "date", "time", "wind_direction", "wind_speed", "temperature", "temperature_dewpoint", "air_pressure")]

WYOMING = rbind(as.matrix(newWY1),as.matrix(newWY2),as.matrix(newWY3),as.matrix(newWY4))
WYOMING <- cbind(WYOMING,State='WYOMING');
head(WYOMING)
tail(WYOMING)
FINAL <-rbind(as.matrix(ALABAMA),as.matrix(ALASKA),as.matrix(ARIZONA),as.matrix(ARKANSAS),
              as.matrix(CALIFORNIA),as.matrix(COLORADO),as.matrix(CONNECTICUT),as.matrix(DELAWARE),
              as.matrix(FLORIDA),as.matrix(GEORGIA),as.matrix(HAWAII),as.matrix(IDAHO),as.matrix(ILLINOIS),
              as.matrix(INDIANA),as.matrix(IOWA),as.matrix(KANSAS),as.matrix(KENTUCKY),as.matrix(LOUISIANA),
              as.matrix(MAINE),as.matrix(MARYLAND),as.matrix(MASSACHUSETTS),as.matrix(MICHIGAN),
              as.matrix(MINNESOTA),as.matrix(MISSISSIPPI),as.matrix(MISSOURI),as.matrix(MONTANA),
              as.matrix(NEBRASKA),as.matrix(NEVADA),as.matrix(NEWHAMPSHIRE),as.matrix(NEWJERSEY),
              as.matrix(NEWMEXICO),as.matrix(NEWYORK),as.matrix(NORTHCAROLINA),as.matrix(NORTHDAKOTA),
              as.matrix(OHIO),as.matrix(OKLAHOMA),as.matrix(OREGON),as.matrix(PENNSYLVANIA),
              as.matrix(RHODEISLAND),as.matrix(SOUTHCAROLINA),as.matrix(SOUTHDAKOTA),as.matrix(TENNESSEE),
              as.matrix(TEXAS),as.matrix(UTAH),as.matrix(VERMONT),as.matrix(VIRGINIA),as.matrix(WASHINGTON),
              as.matrix(WESTVIRGINIA),as.matrix(WISCONSIN),as.matrix(WYOMING))
write.csv(FINAL ,file = "data/WEATHER.csv")

