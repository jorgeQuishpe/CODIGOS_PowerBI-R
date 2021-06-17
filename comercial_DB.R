library(odbc)
library(data.table)
library(DBI)
library(dplyr)
library(lubridate)
library(stringr)
library(readxl)
library(RODBC)


########### CONEXION A BASES DE DATOS
conSRSCreacion <- dbConnect(odbc::odbc(), "conexion_ASTS", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
conSRS<-odbcConnect("conexion_ASTS",uid="sa",pwd="grup03mpresari@lsrs")

############################################################################################################
################################################ REGION ###################################################
region<-as.data.table(dbGetQuery(con44, "SELECT [LAND1],[BLAND],[BEZEI] 
                                   FROM [CHP].[chp].[T005U]"))

names(region)<-c("id_pais","id_region","descripcion_region")
region$id_pais=trimws(region$id_pais)
region$id_region=trimws(region$id_region)
region<-region[!duplicated(paste(region$id_pais,region$id_region)),]
#dbCreateTable(conSRSCreacion, "dim_region", region)

##### regiones nuevas
RegionesExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_pais],[id_region] FROM [ASTS].[dbo].[dim_region]"))
RegionesNuevos<-anti_join(region, RegionesExistentes, by=c("id_pais","id_region"))
sqlSave(conSRS, dat = RegionesNuevos, tablename = "dim_region", rownames = F, append=TRUE)

#################################################### PAISES #################################################
pais<-as.data.table(dbGetQuery(con44, "SELECT [LAND1],[LANDX] 
                                   FROM [CHP].[chp].[T005T]"))

names(pais)<-c("id_pais","nombre_pais")
pais$id_pais=trimws(pais$id_pais)
pais<-pais[!duplicated(pais$id_pais),]
pais$nombre_pais=ifelse(pais$nombre_pais=="Kolumbien", "Colombia", pais$nombre_pais)
#dbCreateTable(conSRSCreacion, "dim_pais", pais)

##### paises nuevos
paisesExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_pais] FROM [ASTS].[dbo].[dim_pais]"))
paisesNuevos<-anti_join(pais, paisesExistentes, by="id_pais")
sqlSave(conSRS, dat = paisesNuevos, tablename = "dim_pais", rownames = F, append=TRUE)

##################################################### MATERIAl ############################################
materiales<-as.data.table(dbGetQuery(con44, "SELECT [MATNR],[MAKTX] 
                                   FROM [CHP].[chp].[MAKT]"))
names(materiales)<-c("id_material","nombre_material")
materiales$id_material=trimws(materiales$id_material)
materiales<-materiales[!duplicated(materiales$id_material),]
#dbCreateTable(conSRSCreacion, "dim_materiales", materiales)

##### materiales nuevos
materialesExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_material] FROM [ASTS].[dbo].[dim_materiales]"))
materialesNuevos<-anti_join(materiales, materialesExistentes, by="id_material")
sqlSave(conSRS, dat = materialesNuevos, tablename = "dim_materiales", rownames = F, append=TRUE)

################################################# SOCIEDAD #############################################
sociedad<-as.data.table(dbGetQuery(con44, "SELECT [BUKRS],[BUTXT],[LAND1]
                                   FROM [CHP].[chp].[T001]"))

names(sociedad)<-c("id_sociedad","nombre_sociedad","id_pas_sociedad")
sociedad$id_sociedad=trimws(sociedad$id_sociedad)
sociedad<-sociedad[!duplicated(sociedad$id_sociedad),]
#dbCreateTable(conSRSCreacion, "dim_sociedad", sociedad)

##### sociedades nuevos
sociedadesExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_sociedad] FROM [ASTS].[dbo].[dim_sociedad]"))
sociedadNuevos<-anti_join(sociedad, sociedadesExistentes, by="id_sociedad")
sqlSave(conSRS, dat = sociedadNuevos, tablename = "dim_sociedad", rownames = F, append=TRUE)


#################################################### ZONA VENTAS ###########################################
zona_ventas<-as.data.table(dbGetQuery(con44, "SELECT [BZIRK],[BZTXT]
                                   FROM [CHP].[chp].[T171T]"))
names(zona_ventas)<-c("id_zonaVenta","descripcion_zonaVenta")
zona_ventas$id_zonaVenta=trimws(zona_ventas$id_zonaVenta)
zona_ventas<-zona_ventas[!duplicated(zona_ventas$id_zonaVenta),]
#dbCreateTable(conSRSCreacion, "dim_zonaVentas", zona_ventas)

##### zona venta nuevos
zonaVenExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_zonaVenta] FROM [ASTS].[dbo].[dim_zonaVentas]"))
zonaVenNuevos<-anti_join(zona_ventas, zonaVenExistentes, by="id_zonaVenta")
sqlSave(conSRS, dat = zonaVenNuevos, tablename = "dim_zonaVentas", rownames = F, append=TRUE)

################################################## OFICINA VENTAS ########################################
oficinaVentas<-as.data.table(dbGetQuery(con44, "SELECT [VKBUR],[BEZEI]
                                   FROM [CHP].[chp].[TVKBT]"))

names(oficinaVentas)<-c("id_oficinaVentas","descripcion_oficinaVentas")
oficinaVentas$id_oficinaVentas=trimws(oficinaVentas$id_oficinaVentas)
oficinaVentas<-oficinaVentas[!duplicated(oficinaVentas$id_oficinaVentas),]
#dbCreateTable(conSRSCreacion, "dim_oficinaVentas", oficinaVentas)

##### oficina venta nuevos
oficiVenExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_oficinaVentas] FROM [ASTS].[dbo].[dim_oficinaVentas]"))
oficiVenNuevos<-anti_join(oficinaVentas, oficiVenExistentes, by="id_oficinaVentas")
sqlSave(conSRS, dat = oficiVenNuevos, tablename = "dim_oficinaVentas", rownames = F, append=TRUE)

###############################################  VENDEDORES ################################################
Vendedores<-as.data.table(dbGetQuery(con44, "SELECT [PERNR],[NACHN],[VORNA]
                                   FROM [CHP].[chp].[PA0002]"))
Vendedores$nombre_vendedor = paste(Vendedores$VORNA,Vendedores$NACHN)
Vendedores<-Vendedores[,c(1,4)]
names(Vendedores)<-c("id_vendedor","nombre_vendedor")
Vendedores$id_vendedor=trimws(Vendedores$id_vendedor)
Vendedores<-Vendedores[!duplicated(Vendedores$id_vendedor),]
#dbCreateTable(conSRSCreacion, "dim_vendedores", Vendedores)

##### grupo vendedor nuevos
VendedExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_vendedor] FROM [ASTS].[dbo].[dim_vendedores]"))
VendedNuevos<-anti_join(Vendedores, VendedExistentes, by="id_vendedor")
sqlSave(conSRS, dat = VendedNuevos, tablename = "dim_grupoVendedor", rownames = F, append=TRUE)

########################################### CORRECCION CARGA CEBE ##########################################
Cebe<-as.data.table(dbGetQuery(con44, "SELECT * FROM [CHP].[chp].[CEPCT] WHERE KOKRS = '1000'"))
Cebe<-Cebe[,c("PRCTR","LTEXT")]
names(Cebe)<-c("id_cebe","descripcion_cebe")
Cebe$id_cebe=trimws(Cebe$id_cebe)
Cebe<-Cebe[!duplicated(Cebe$id_cebe),]
Cebe$descripcion_cebe<-ifelse(Cebe$id_cebe=="0000011000", "IMPERMEABILIZACIÓN",
                              ifelse(Cebe$id_cebe=="0000026000", "RINNOVA",
                                     ifelse(Cebe$id_cebe=="0000018000", "ARQUITECTÓNICOS",
                                            ifelse(Cebe$id_cebe=="0000017000", "ACRÍLICOS",
                                                   ifelse(Cebe$id_cebe=="0000027000", "MAPEI",
                                                          ifelse(Cebe$id_cebe=="0000013000", "NO EXISTE CENTRO",
                                                                 ifelse(Cebe$id_cebe=="0000016000", "NO EXISTE CENTRO",
                                                                        ifelse(Cebe$id_cebe=="0000019000", "NO EXISTE CENTRO",
                                                                               ifelse(Cebe$id_cebe=="0000020000", "NO EXISTE CENTRO",
                                                                                      ifelse(Cebe$id_cebe=="0000028000", "NO EXISTE CENTRO",
                                                                                             ifelse(Cebe$id_cebe=="0000015000", "VIALES","OTRO")))))))))))
#dbCreateTable(conSRSCreacion, "dim_Cebe", Cebe)

##### Cebe nuevos
CebeExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_cebe] FROM [ASTS].[dbo].[dim_cebe]"))
CebeNuevos<-anti_join(Cebe, CebeExistentes, by="id_cebe")
sqlSave(conSRS, dat = CebeNuevos, tablename = "dim_cebe", rownames = F, append=TRUE)




###################################################################################################################################################################################################
###################################################################################################################################################################################################
conSRSCreacion <- dbConnect(odbc::odbc(), "conexion_ASTS", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
conSRS<-odbcConnect("conexion_ASTS",uid="sa",pwd="grup03mpresari@lsrs")

########################################################## CLIENTES #################################################
clientes<-as.data.table(dbGetQuery(con44, "SELECT [NAME1],[NAME2],[KUNNR],[REGIO],[LAND1],[KTOKD] 
                                   FROM [CHP].[chp].[KNA1]"))
clientes$nombre<-paste(clientes$NAME1, clientes$NAME2, sep = " ")
clientes<-clientes[,c("KUNNR","nombre","REGIO","LAND1","KTOKD")]
names(clientes)<- c("id_cliente","nombre", "id_region","id_pais","Gr_Cuentas")
clientes$id_cliente=trimws(clientes$id_cliente)
clientes<-clientes[!duplicated(clientes$id_cliente),]
#dbCreateTable(conSRSCreacion, "dim_clientes",clientes)

##### clientes nuevos
clientesExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [id_cliente] FROM [ASTS].[dbo].[dim_clientes]"))
clientesNuevos<-anti_join(clientes, clientesExistentes, by="id_cliente")
sqlSave(conSRS, dat = clientesNuevos, tablename = "dim_clientes", rownames = F, append=TRUE)


########################################################## VENTAS #################################################
con44<-dbConnect(odbc::odbc(), "con_prod", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
ventas<-as.data.table(dbGetQuery(con44, "SELECT [BELNR],[RBELN],[VRGAR],[FADAT],[BUDAT],[KNDNR],[ARTNR],[FKART],[BUKRS],[WERKS],[GSBER],[SPART],[PRCTR],[PPRCTR],
[BZIRK],[KDGRP],[LAND1],[MATKL],[PAPH2],[PAPH3],[SEGMENT],[VKBUR],[VKGRP],[VRTNR],[VBELN],[KTOKD],[VRPRS],
[KWSKTO],[VVMAT],[KWKDRB],[VVUME],[VVDPV],[ABSMG_ME],[VVUME_ME],[ABSMG],[ERLOS] ,[KWFRGR], [KWINSR],[VVFGS],
[KWMGRB], [MRABA],[VVDPE],[VVDPD],
[PERIO],[HZDAT],[GJAHR],[VKORG],[PAPH1]
FROM [CHP].[chp].[CE1GSRS]
WHERE BUKRS='1000'"))
#WHERE BUKRS='1000' and VRGAR='F'"))

ventas <- mutate_at(ventas, c("ERLOS","KWFRGR","KWINSR","VVFGS","KWSKTO","KWKDRB","KWMGRB","MRABA","VVDPV","VVDPE","VVDPD"), ~replace(., is.na(.), 0))

ventas$ingresos_brutos<-ventas$ERLOS + ventas$KWFRGR + ventas$KWINSR - ventas$VVFGS
ventas$descuentos_brutos<-ventas$KWSKTO + ventas$KWKDRB + ventas$KWMGRB + ventas$MRABA + ventas$VVDPV + ventas$VVDPE + ventas$VVDPD
ventas$ingresos_netos<-((ventas$ERLOS + ventas$KWFRGR + ventas$KWINSR - ventas$VVFGS) +
                          (ventas$KWSKTO + ventas$KWKDRB + ventas$KWMGRB + ventas$MRABA + ventas$VVDPV + ventas$VVDPE + ventas$VVDPD))


ventas<-ventas[,c("BELNR","RBELN","VRGAR","FADAT","BUDAT","KNDNR","ARTNR","FKART","BUKRS","WERKS","GSBER","SPART","PRCTR","PPRCTR",
                  "BZIRK","KDGRP","LAND1","MATKL","PAPH2","PAPH3","SEGMENT","VKBUR","VKGRP","VRTNR","VBELN","KTOKD","VRPRS","KWFRGR",
                  "KWSKTO","VVMAT","KWKDRB","KWINSR","VVUME","VVDPV","ABSMG_ME","VVUME_ME","ABSMG","ingresos_brutos","descuentos_brutos","ingresos_netos",
                  "PERIO","HZDAT","GJAHR","VKORG","PAPH1")]

names(ventas)<-c("num_documento","documento_referencia","tipo_documento","fecha_factura","Fecha_Contable","id_cliente","id_material","Cl_Factura","id_sociedad",
                 "centro","division","sector","id_cebe","CeBe_Int","id_zonaVenta","id_grupoCliente","id_pais","id_grupoMaterial","grupo_tipo_producto","marca_producto","segmento",
                 "id_oficinaVentas","id_grupoVendedor","id_vendedor","codigo_documento_ventas","id_grupoCuentas","valor_Imputado","ingresos_transporte","descuento_pronto_pago","costo_material","descuento_cliente",
                 "seguro_por_seguro","cantidad_facturada","descuento_por_volumen","volumen_ventas_um","cantidad_facturada_umb","volumen_ventas","ingresos_brutos","descuentos_brutos","ingresos_netos",
                 "Periodo","FechaCreacion","Año","OrganizacionVentas","DivisionProducto")

ventas$id_cebe=trimws(ventas$id_cebe)
ventas$id_cliente=trimws(ventas$id_cliente)
ventas$id_grupoCliente=trimws(ventas$id_grupoCliente)
ventas$id_grupoCuentas=trimws(ventas$id_grupoCuentas)
ventas$id_grupoVendedor=trimws(ventas$id_grupoVendedor)
ventas$id_material=trimws(ventas$id_material)
ventas$id_oficinaVentas=trimws(ventas$id_oficinaVentas)
ventas$id_pais=trimws(ventas$id_pais)
ventas$id_sociedad=trimws(ventas$id_sociedad)
ventas$id_vendedor=trimws(ventas$id_vendedor)
ventas$id_zonaVenta=trimws(ventas$id_zonaVenta)

ventas$fecha_factura=paste(substr(ventas$fecha_factura, start = 1, stop = 4),
                           substr(ventas$fecha_factura, start = 5, stop = 6),
                           substr(ventas$fecha_factura, start = 7, stop = 8), sep = "/")
ventas$fecha_factura=as.Date(ventas$fecha_factura,format="%Y/%m/%d")

ventas$Fecha_Contable=paste(substr(ventas$Fecha_Contable, start = 1, stop = 4),
                            substr(ventas$Fecha_Contable, start = 5, stop = 6),
                            substr(ventas$Fecha_Contable, start = 7, stop = 8), sep = "/")
ventas$Fecha_Contable=as.Date(ventas$Fecha_Contable,format="%Y/%m/%d")

ventas$FechaCreacion=paste(substr(ventas$FechaCreacion, start = 1, stop = 4),
                           substr(ventas$FechaCreacion, start = 5, stop = 6),
                           substr(ventas$FechaCreacion, start = 7, stop = 8), sep = "/")
ventas$FechaCreacion=as.Date(ventas$FechaCreacion,format="%Y/%m/%d")

ventas$cantidad_facturada_umb=recode_factor(ventas$cantidad_facturada_umb,
                                            "ST"="UN",
                                            "BOT"="LC",
                                            "GLL"="GLN",
                                            "BAG"="BOL",
                                            "CS"="CJ")

ventas[is.na(ventas$costo_material)]<-0
ventas[is.na(ventas$valor_Imputado)]<-0
ventas$descuentos_brutos<-as.numeric(ventas$descuentos_brutos)
ventas <- mutate_at(ventas, c("descuentos_brutos","ingresos_brutos","ingresos_netos"), ~replace(., is.na(.), 0))

#ventas$costo_venta=ventas$costo_material+ventas$valor_Imputado
ventas$costo_venta=ventas$costo_material+ventas$valor_Imputado+ventas$descuentos_brutos

ventas$centro<-recode_factor(ventas$centro,
                             '1000'='CASA IMPTEK CASHAPAMBA',
                             '1100'='CASA IMPTEK CASHAPAMBA',
                             '1200'='CASA IMPTEK LA NAVAL',
                             '1300'='CASA IMPTEK CUMBAYÁ',
                             '1600'='CASA IMPTEK VÍA A LA COSTA')
ventas$Año=as.integer(ventas$Año)
ventas$Mess=as.integer(substr(ventas$Periodo,start = 5,stop = 7))
ventas$Periodo=format(as.Date(paste(as.integer(substr(ventas$Periodo, start = 1, stop = 4)),
                                    as.integer(substr(ventas$Periodo, start = 5, stop = 7)),1,sep = "/"),format = "%Y/%m/%d"),"%b")
ventas$Periodo=substr(ventas$Periodo,start = 1,stop = 3)
names(ventas)[names(ventas)=="Periodo"]<-"mes"

###### carga de transaccioes nuevas
conSRSCreacion <- dbConnect(odbc::odbc(), "conexion_ASTS", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
ventasExistentes<-as.data.table(dbGetQuery(conSRSCreacion, "SELECT [num_documento],[documento_referencia] FROM [ASTS].[dbo].[hechos_ventas]"))
ventasNuevas<-anti_join(ventas, ventasExistentes, by="num_documento")

#dbCreateTable(conSRSCreacion, "hechos_ventas", ventas)
conSRS<-odbcConnect("conexion_ASTS",uid="sa",pwd="grup03mpresari@lsrs")
sqlSave(conSRS, dat = ventasNuevas, tablename = "hechos_ventas", rownames = F, append=TRUE)

#####################################################################################################################
print(paste("proceso carga de datos Comercial finalizada con exito, fecha: ", now(), sep = ""))

