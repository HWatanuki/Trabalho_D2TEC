#OPTION('OutputLimitMB',1000);
IMPORT STD;

// Declaração do schema dos dados
Layout := RECORD
    STRING VendorID;  // codigo indicando a companhia associada a viagem
    STRING tpep_pickup_datetime; // data e hora do embarque
    STRING tpep_dropoff_datetime; // data e hora do desembarque
    STRING passenger_count; // numero de passageiros
    STRING trip_distance; // distancia da viagem
    STRING RatecodeID; // codigo final de cobranca da viagem
    STRING store_and_fwd_flag; // codigo que indica se os dados da viagem foram gravados no veiculo por falta de conexao
    STRING PULocationID; // codigo do local de embarque
    STRING DOLocationID; // codigo do local de desembarque
    STRING payment_type; // tipo do pagamento (dinheiro,cartao,etc)
    STRING fare_amount; // valor da corrida no taximetro
    STRING extra; // tarifas extras nos horarios de pico
    STRING mta_tax; // imposto extra em funcao da taxa do taximetro
    STRING tip_amount; // valor da gorjeta
    STRING tolls_amount; // valor dos pedagios
    STRING improvement_surcharge; // taxa compensatoria para viagens curtas
    STRING total_amount; // valor total recebido do passageiro
 END;
 
 Layout_zone := RECORD 
  UNSIGNED2 LocationID; // Codigo identificador das zonas de taxi
  STRING Borough; // Bairro da zona
  STRING Zone; // Nome da zona
  STRING service_zone; // Categoria de servico da zona
 END;
 
// Declaração do dataset de viagens
File_201701 := DATASET('~d2tec::yellow_tripdata_2017_01',Layout,CSV(HEADING(1)));
OUTPUT(CHOOSEN(File_201701,100), NAMED('Dataset_Raw'));
COUNT(File_201701);

File_Zone:= DATASET('~d2tec::taxi_zone_lookup',Layout_zone,CSV(HEADING(1)));
OUTPUT(File_Zone, NAMED('Dataset_Zone')); 
COUNT(File_Zone);

//1 -- Limpeza e padronização dos dados
 Layout_Clean := RECORD
   UNSIGNED8 RecID;
   UNSIGNED1 vendorid;
// STRING19 tpep_pickup_datetime;
   Std.Date.Date_t tpep_pickup_date; 
   Std.Date.Time_t tpep_pickup_time;
// STRING19 tpep_dropoff_datetime; 
   Std.Date.Date_t tpep_dropoff_date; 
   Std.Date.Time_t tpep_dropoff_time;
   UNSIGNED1 passenger_count;
// REAL4 trip_distance; 
   DECIMAL5_2 trip_distance;
   UNSIGNED1 ratecodeid;
   STRING1   store_and_fwd_flag; 
   STRING pulocationid; 
   STRING dolocationid; 
   UNSIGNED1 payment_type;
// REAL8 fare_amount; 
   DECIMAL9_2 fare_amount; 
// REAL4 extra;
   DECIMAL5_2 extra;
// REAL4 mta_tax;
   DECIMAL5_2 mta_tax;
// REAL4 tip_amount;
   DECIMAL5_2 tip_amount;
// REAL4 tolls_amount;
   DECIMAL5_2 tolls_amount;
// REAL4 improvement_surcharge;
   DECIMAL3_2 improvement_surcharge;
// REAL8 total_amount;
   DECIMAL9_2 total_amount;
  END;

Layout_Clean CleanData(File_201701 Le, INTEGER cnt) := TRANSFORM
 //Identificador único
 SELF.RecID                 := cnt;
 //Tratamento dos campos de data e hora
 SELF.tpep_pickup_date      := Std.Date.FromStringToDate(Le.tpep_pickup_datetime[1..10],'%Y-%m-%d');
 SELF.tpep_pickup_time      := Std.Date.FromStringToTime(Le.tpep_pickup_datetime[12..],'%H:%M:%S');
 SELF.tpep_dropoff_date     := Std.Date.FromStringToDate(Le.tpep_dropoff_datetime[1..10],'%Y-%m-%d');
 SELF.tpep_dropoff_time     := Std.Date.FromStringToTime(Le.tpep_dropoff_datetime[12..],'%H:%M:%S');
 //Alteração e delimitação dos tipos de dados 
 SELF.vendorid              := (UNSIGNED1)Le.vendorid;
 SELF.passenger_count       := (UNSIGNED1)Le.passenger_count;
 SELF.trip_distance         := (DECIMAL5_2)Le.trip_distance;
 SELF.ratecodeid            := (UNSIGNED1)Le.ratecodeid;
 SELF.store_and_fwd_flag    := (STRING1)Le.store_and_fwd_flag;
 SELF.payment_type          := (UNSIGNED1)Le.payment_type;
 SELF.fare_amount           := (DECIMAL9_2)Le.fare_amount;
 SELF.extra                 := (DECIMAL5_2)Le.extra;
 SELF.mta_tax               := (DECIMAL5_2)Le.mta_tax;
 SELF.tip_amount            := (DECIMAL5_2)Le.tip_amount;
 SELF.tolls_amount          := (DECIMAL5_2)Le.tolls_amount;
 SELF.improvement_surcharge := (DECIMAL3_2)Le.improvement_surcharge;
 SELF.total_amount          := (DECIMAL9_2)Le.total_amount;
 SELF                       := Le;
END; 

CleanRecs := PROJECT(File_201701,CleanData(LEFT,COUNTER)):PERSIST('~clean_data');
OUTPUT(CleanRecs, NAMED('Dataset_Clean'));
COUNT(CleanRecs);

//2 -- JOIN do dataset de viagens com o dataset contendo os nomes das localidades de NY
Join1 := JOIN(CleanRecs,File_Zone(Borough<>'Unknown'),LEFT.puLocationID=(STRING)RIGHT.LocationID,
																	 TRANSFORM(RECORDOF(LEFT),
                                            SELF.puLocationID:=RIGHT.Zone;
																						SELF:=LEFT));
FullData := JOIN(Join1,File_Zone(Borough<>'Unknown'),LEFT.doLocationID=(STRING)RIGHT.LocationID,
																	 TRANSFORM(RECORDOF(LEFT),
                                            SELF.doLocationID:=RIGHT.Zone;
																						SELF:=LEFT)):PERSIST('fulldata');																		
																	
OUTPUT(FullData, NAMED('Dataset_Joined'));																					
COUNT(FullData);																					

//3 -- Cobertura do dataset
UniqueItineraries := TABLE(FullData,{puLocationid,doLocationid,cnt:=COUNT(GROUP)},puLocationid,doLocationid);
CntUniqueItineraries := COUNT(UniqueItineraries);
CntUniqueItineraries;
Coverage := (CntUniqueItineraries/2.620E+521)*100;  //2.620E+521 corresponde ao total de combinaçoes possiveis entre as areas de NY
OUTPUT(Coverage,NAMED('Cobertura_Dataset'));

//4 -- Dominio de um vendor 
VendorDist := TABLE(FullDAta,{vendorid, Pct := ROUND(COUNT(GROUP)/COUNT(FullData)*100)},vendorid); 
OUTPUT(VendorDist, NAMED('Viagens_Vendor'));

//5 -- Trajetos mais frequentes ao longo do mes
TopItineraries := CHOOSEN(SORT(UniqueItineraries,-cnt),10);
OUTPUT(TopItineraries,NAMED('Trajetos_frequentes'));

//6 -- Distribuicao das viagens ao longo dos dias do mes
TripDayProfile := TABLE(FullData,{tpep_pickup_date,cnt:=COUNT(GROUP)},tpep_pickup_date);
OUTPUT(TripDayProfile,NAMED('Viagens_por_dia'));

//7 -- Distribuiçao das viagens ao longo das horas dos dias
TripHourProfile := SORT(TABLE(FullData,{hora:=INTFORMAT(tpep_pickup_time,6,1)[..2],
                                       // DECIMAL5_2 pct:=(COUNT(GROUP)/COUNT(FullData))*100},
                                       cnt:=COUNT(GROUP)},
																			 INTFORMAT(tpep_pickup_time,6,1)[..2]),
												// -pct);
												hora);
OUTPUT(TripHourProfile,NAMED('Viagens_por_hora'));

// 8 -- Trajetos com viagens medias mais caras
CostProfile :=       SORT(
										      TABLE(FullData,{puLocationID,
                                          doLocationID,
																		      trip_count:=COUNT(GROUP),
																		      avg_amount:=ROUND(AVE(GROUP,total_amount))},
													puLocationID,doLocationID),
                      -avg_amount);
OUTPUT(CHOOSEN(CostProfile(trip_count>=30),10),NAMED('Trajetos_mais_caros'));

//9 -- Estimativa de gorjetas por regiao

TipProfile := TABLE(FullData,{puLocationID,
                              trip_count:=COUNT(GROUP);
															avg_tip:=ROUND(AVE(GROUP,tip_amount)); 
                              sum_tip:=ROUND(SUM(GROUP,tip_amount))},
										puLocationID);
                      
OUTPUT(CHOOSEN(SORT(TipProfile,-sum_tip),10),NAMED('Gorjeta_Regiao'));

//10 -- Correlacao da gorjeta com a distancia e custo da viagem

DistCorr:= CORRELATION(FullData,tip_amount,trip_distance);
FareCorr:= CORRELATION(FullData,tip_amount,fare_amount);
CorrAnalyz := DATASET([{'trip_distance',DistCorr},
											 {'fare_amount',FareCorr}], {STRING fator, DECIMAL3_2 correlacao});
OUTPUT(CorrAnalyz,NAMED('Correlacao_gorjeta'));

//11 -- Funcao para estimativa dos valores médios de cada trajeto, dia da semana e hora do dia

Tbl1 := TABLE(FullData(tpep_pickup_date=tpep_dropoff_date),
             {RecID,trip_distance,puLocationID,doLocationID,fare_amount, 
              UNSIGNED1 puHour   := Std.Date.Hour(tpep_pickup_time), 
              UNSIGNED1 puDOW    := Std.Date.DayOfWeek(tpep_pickup_date),
              UNSIGNED8 Duration := (tpep_dropoff_time - tpep_pickup_time)}); 

OUTPUT(Tbl1, NAMED('Vertical_slice'));
COUNT(Tbl1);

AvgData := TABLE(Tbl1(Duration<>0,trip_distance<>0),
                 {puLocationID,doLocationID,puDOW,puHour, 
                  GrpCnt      := COUNT(GROUP),
                  AvgDistance := (DECIMAL5_2)AVE(GROUP,trip_distance),
                  AvgFare     := (DECIMAL7_2)AVE(GROUP,fare_amount),
                  AvgDuration := (DECIMAL7_2)(AVE(GROUP,Duration))}, 
									puLocationID,doLocationID,puDOW,puHour):PERSIST('avgdata');
OUTPUT(AvgData, NAMED('Dados_medios'));
COUNT(AvgData);

TaxiDataSvc(STRING pickup,STRING dropoff, UNSIGNED1 dow,UNSIGNED1 hour) := FUNCTION 

 FilterRecs:= AvgData(puLocationID = pickup AND doLocationID = dropoff AND puDOW = dow AND puHour = hour);

 DayStr(UNSIGNED1 d) := CHOOSE(d,'Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday',''); 
 HrStr(UNSIGNED1 h)  := CHOOSE(h+1,'Midnight','1 AM','2 AM','3 AM','4 AM','5 AM','6 AM','7 AM',
                                  '8 AM','9 AM','10 AM','11 AM','Noon','1 PM','2 PM','3 PM',
                                  '4 PM','5 PM','6 PM','7 PM','8 PM','9 PM','10 PM','11 PM',
                              '');
OutRec := RECORD 
  STRING25 Pickup; 
  STRING25 Dropoff; 
  STRING10 Day; 
  STRING10 Hour; 
  DECIMAL7_2 duration; 
  DECIMAL7_2 fare; 
  DECIMAL5_2 distance;
END;

OutRec XF(FilterRecs Le) := TRANSFORM
 SELF.Pickup   := Le.puLocationID;
 SELF.Dropoff  := Le.doLocationID; 
 SELF.Day      := DayStr(Le.puDOW);
 SELF.Hour     := HrStr(Le.puHour); 
 SELF.Duration := Le.avgDuration;
 SELF.Fare     := Le.avgFare;
 SELF.Distance := Le.avgDistance;
END;

RETURN PROJECT(FilterRecs,XF(LEFT));
END;

TaxiDataSvc('JFK Airport','Newark Airport',1,5);
