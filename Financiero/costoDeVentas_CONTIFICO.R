#install.packages("BBmisc")
#librerias
library(odbc)
library(data.table)
library(DBI)
library(dplyr)
library(lubridate)
library(stringr)
library(readxl)
library(RODBC)
library(BBmisc)

# CONEXIONES A BASE DE DATOS
CODISNA_creacion <- dbConnect(odbc::odbc(), "conCODISNA", UID="sa", PWD= "grup03mpresari@lsrs", enconding='UTF-8')
CODISNA_carga<-odbcConnect("conCODISNA",uid="sa",pwd="grup03mpresari@lsrs")

####################################################################################################################
# lectura de excels
setwd("C:/Users/jquishpe/Downloads")
ficheros <- list.files(pattern = "\\.xls")
ficheros<- substring(ficheros, first = 20)
# en la siguiente linea restar en marzo 11, abril 9, mayo 16
catProd<- substr(ficheros,1,nchar(ficheros)-16)
catProd<-catProd[-1]


####### lectura de la tabla completa  #######
fechaCarga=Sys.Date()-1
tablaCompleta <- as.data.table(read_excel(paste0("C:/Users/jquishpe/Downloads/ReporteCostosVenta_",fechaCarga, "_.xls"), skip = 4))

nfilas=nrow(tablaCompleta)
tablaCompleta<-tablaCompleta[c(-1,-nfilas),]

####### bucle
rep_costoVenta=data.table()
for (i in catProd) {
  #direccion=paste0("C:/Users/jquishpe/Downloads/ReporteCostosVenta_", i, "_marzo_.xls")
  #direccion=paste0("C:/Users/jquishpe/Downloads/ReporteCostosVenta_", i, "_Abr_.xls")
  direccion=paste0("C:/Users/jquishpe/Downloads/ReporteCostosVenta_", i,"_",fechaCarga, "_.xls")
  datos <- as.data.table(read_excel(direccion, skip = 4))
  nfilas=nrow(datos)
  datos<-datos[c(-1,-nfilas),]
  
  if (dim(datos)[1]==dim(tablaCompleta)[1]){
    print(paste("Tabla:",i,"con numero de filas inconsistentes"))
  }else{
    
  }
  
  datos$catProd=i
  #datos$fecha=as.Date("2021/03/01", format="%Y/%m/%d")
  #datos$fecha=as.Date("2021/04/01", format="%Y/%m/%d")
  datos$fecha=fechaCarga
  rep_costoVenta=rbind(datos, rep_costoVenta)
}

names(rep_costoVenta)<-c("codigo","nombre", "unidad", "PVP_promedio", "costoVentaProm","margenUnitario",
                         "porc_costoUnitario", "cantidad","totalVentas", "totalCosto", "margenTotal",
                         "porc_costo", "categoriaProd", "fecha")

rep_costoVenta <- mutate_at(rep_costoVenta, c("PVP_promedio","costoVentaProm"), ~replace(., is.na(.), 0))
rep_costoVenta$Porc_margen<-as.numeric((rep_costoVenta$PVP_promedio-rep_costoVenta$costoVentaProm)/rep_costoVenta$PVP_promedio)
rep_costoVenta$Porc_margen=ifelse(rep_costoVenta$Porc_margen==-Inf, 0,rep_costoVenta$Porc_margen)
rep_costoVenta<-rep_costoVenta[,c(13,1:12,15,14)]

################################################################ Carga a la base de datos ################################################## 
# dbCreateTable(CODISNA_creacion, "rep_costoVenta", rep_costoVenta)

if (dim(rep_costoVenta)[1]==dim(tablaCompleta)[1]){
  
  sqlSave(CODISNA_carga, dat = rep_costoVenta, tablename = "rep_costoVenta", rownames = F, append=TRUE)
  print(paste("mismo numero de filas, se inserta todo, ejecutado el:", now()))
  
}else{
  print(paste("diferente numero de filas, error de carga ejecutado el:", now()))
  
  catProdCompleta = c("LICORES", "CARNES","SALSAS Y ADEREZOS","GALLETAS","ASEO DEL HOGAR","ABARROTES",
                      "PASTA Y FIDEOS","ACEITES","CONDIMENTOS","VEGETALES","FRUTAS","CHOCOLATES","CARAMELOS Y GOMAS DE MASCAR",
                      "CAFE Y TÉ","LÁCTEOS","YOGURT","QUESO","PANADERÍA","CEREALES","FRUTOS SECOS NATURALES","SNACKS",
                      "EMBUTIDOS","ASEO PERSONAL","BEBIDAS ENERGIZANTES","BEBIDAS Y GASEOSAS","JUGOS NATURALES",
                      "HELADOS","TORTAS Y POSTRES","MARISCOS","CERVEZA","VINOS","MASCOTAS","COMIDA RAPIDA",
                      "CONSERVAS Y ACEITUNAS","OPCIONAL","NOVEDADES","PROCESADOS CONGELADOS","MATERIA PRIMA CONSUMO MÁQUINAS",
                      "PRODUCTOS NATURALES","DELIVERY","PROMOCIÓN","CIGARRILLOS","LIBROS")
  print(setdiff(catProdCompleta, catProd))
    
}



#################################################################################################################################################### 
##################################### CODIGO PARA ALMACENAR UNA TABLE POR MES

# if (dim(rep_costoVenta)[1]==dim(tablaCompleta)[1] & diaCarga==1){
#   
#   sqlSave(CODISNA_carga, dat = rep_costoVenta, tablename = "rep_costoVenta", rownames = F, append=TRUE)
#   print(paste("mismo numero de filas dia 1, no borra nada e inserta todo del dia 1, ejecutado el:", now()))
#   
# }else{
#   if (dim(rep_costoVenta)[1]==dim(tablaCompleta)[1]& diaCarga!=1){
#     datos<-as.data.table(dbGetQuery(CODISNA_creacion, "SELECT * FROM rep_costoVenta"))
#     fechaMax<-max(datos$fecha)
#     query<-paste0("DELETE FROM rep_costoVenta where fecha='",fechaMax,"'")
#     dbSendQuery(CODISNA_creacion, query)
#     sqlSave(CODISNA_carga, dat = rep_costoVenta, tablename = "rep_costoVenta", rownames = F, append=TRUE)
#     print(paste("mismo numero de filas, dia diferente de 1 por lo que borra el dia anterior, ejecutado el:", now()))
#     
#   }else{
#     print(paste("diferente numero de filas, error de carga ejecutado el:", now()))
#     
#     catProdCompleta = c("LICORES", "CARNES","SALSAS Y ADEREZOS","GALLETAS","ASEO DEL HOGAR","ABARROTES",
#                         "PASTA Y FIDEOS","ACEITES","CONDIMENTOS","VEGETALES","FRUTAS","CHOCOLATES","CARAMELOS Y GOMAS DE MASCAR",
#                         "CAFE Y TÉ","LÁCTEOS","YOGURT","QUESO","PANADERÍA","CEREALES","FRUTOS SECOS NATURALES","SNACKS",
#                         "EMBUTIDOS","ASEO PERSONAL","BEBIDAS ENERGIZANTES","BEBIDAS Y GASEOSAS","JUGOS NATURALES",
#                         "HELADOS","TORTAS Y POSTRES","MARISCOS","CERVEZA","VINOS","MASCOTAS","COMIDA RAPIDA",
#                         "CONSERVAS Y ACEITUNAS","OPCIONAL","NOVEDADES","PROCESADOS CONGELADOS","MATERIA PRIMA CONSUMO MÁQUINAS",
#                         "PRODUCTOS NATURALES","DELIVERY","PROMOCIÓN","CIGARRILLOS","LIBROS")
#     print(setdiff(catProdCompleta, catProd))
#     
#   }
# }


