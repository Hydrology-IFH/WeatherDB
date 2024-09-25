# Method

Behind this package/website is a PostGreSQL-database. This database is build and updated with the same package, as for downloading the data. The only difference is, that the given connection in the secretSettings file needs to have write permissions for the database.
Therefor everyone can look into the code to find out exactly how the database creation works. But as an overview this page will give basic explanations of the processes behind it.

The timeseries for Temperature, Evapotranspiration and Precipitation are going through a 3-4 step process.The result of every process is saved and can get downloaded, with the corresponding abbreviation:
- downloading the raw data --> "raw"
- quality check the data --> "qc"
- fillup the data --> "filled"
- richter correct the values --> "corr"

In the following chapters the processes will get explained furthermore.

## downloading the data

The raw data is downloaded from the [DWD-CDC server](https://opendata.dwd.de/climate_environment/CDC/). The timeseries are downloaded and saved from the 1.1.1994 on. If there is historical data available for a measurement, they are preferred over recent values, because they are already quality checked a bit. The Temperature (T) and potential Evapotranspiration (ET) is downloaded on daily resolution. Where as the Precipitation (N) is downloaded as 10 minute and daily values, but only the 10 minute values are the basis for the downloads.

**Table 1: The downloaded raw data, resolution and their source**
| parameter | resolution | <div style="text-align: center">source</div> |
|:---:|:---:|---|
| Temperature | daily | <ul><li><div>DWD Climate Data Center (CDC):<br> Historical daily station observations (temperature, pressure, precipitation, sunshine duration, etc.) for Germany, version v21.3, 2021, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/) </div></li><li><div>DWD Climate Data Center (CDC): <br>Recent daily station observations (temperature, pressure, precipitation, sunshine duration, etc.) for Germany, quality control not completed yet, version recent, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/)</div></li></ul>||
| pot. Evapotranspiration | daily | <ul><li><div>DWD Climate Data Center: <br>Berechnete historische Tageswerte von charakteristischen Elementen aus dem Boden und dem Pflanzenbestand, historic values, Version v2, parameter "VPGFAO"<br>[online available](https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/daily/historical/) </div></li><li><div>DWD Climate Data Center: <br>Berechnete historische Tageswerte von charakteristischen Elementen aus dem Boden und dem Pflanzenbestand, recent values, Version v2, parameter "VPGFAO"<br>[online available](https://opendata.dwd.de/climate_environment/CDC/derived_germany/soil/daily/recent/)</div></li></ul>|
| Precipitation | 10 minutes | <ul><li><div>DWD Climate Data Center (CDC): <br>Historical 10-minute station observations of precipitation for Germany, version V1, 2019, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/historical/)</div></li><li><div>DWD Climate Data Center (CDC): <br>Recent 10-minute station observations of precipitation for Germany,version recent, 2019, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/recent)</div></li></ul>|
| Precipitation | daily | <ul><li><div>DWD Climate Data Center (CDC): <br>Historical daily station observations (temperature, pressure, precipitation, sunshine duration, etc.) for Germany, version v21.3, 2021, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical/) </div></li><li><div>DWD Climate Data Center (CDC): <br>Recent daily station observations (temperature, pressure, precipitation, sunshine duration, etc.) for Germany, quality control not completed yet, version recent, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/)</div></li></ul>||

For computation improvements the downloaded files and their modification time is saved to the database, to be able to only download the updated data.

## quality check

To quality check the data it is very dependent on which parameter is treated. Therefor this chapter is grouped into subchapters.

Although every quality check can get computed for different periods:
- on all of the data by using e.g. `station.StationT(3).quality_check()`
- only a specified period, with e.g. `station.StationP(3).quality_check(period=("2010-01-01", "2020-12-31"))`
- the last imported period, with e.g. `station.StationET(3).last_imp_quality_check()`

### Temperature and Evapotranspiration

For T and ET quality check the data is compared to the 5 nearest neighbors data.
To get the nearest stations, the selection is done on a yearly basis. For this neighbor stations having data for more than 6 months in that year are considered.

Furthermore also the difference in elevation between the two stations is considered to select the closest stations. This is done, because the topographie has a big influence on the Temperature and Evapotranspiration. The weighted distance is thereby calculated with the LARSIM formula:

$L_{gewichtet} = L_{horizontal} * (1 + (\frac{|\delta H|}{P_1})^{P_2})$

In Larsim those parameters are defined as $P_1 = 500$ and $P_2 = 1$.
<br>Stoelzle et al. (2016) found that $P_1 = 100$ and $P_2 = 4$ is better for Baden-Württemberg to consider the quick changes in topagraphie.
<br>or all of Germany, those parameter values are giving too much weight to the elevation difference, which can result in getting neighbor stations from the border of the Czech Republic for the Feldberg station. Therefor the values $P_1 = 250$ and $P_2 = 1.5$ are used as default values.

The data of every neighbor station is regionalized, based on the DWD grids (see chapter regionalisation), to the examined station. Then the mean value of those 5 values is taken to compare to the station data. If this mean value is too far away from the own value, then the measurement point is deleted.

**Table 2: Limits for the quality check of the T and ET measurements, when compared with neighbor stations**
| parameter | compare equation | lower limit | upper limit| treshold |
|:---:|:---:|:---:|:---:|:---:|
| Temperature |  $\Delta T = T_{Stat} - \overline{T}_{neighbors}$ | $\Delta T < -5°C$ | $\Delta T > 5°C$ | $-50°C < T_{Stat} < 50°C$ |
| pot. Evapotranspiration |  $\delta ET = \dfrac{ET_{Stat}}{\overline{ET}_{neighbors}}$ | $\begin{cases}\delta ET< 25\% \\ ET_{Stat}> 2 \frac{mm}{d}\end{cases}$|$\begin{cases}\delta ET> 200\% \\ ET_{Stat}> 3 \frac{mm}{d}\end{cases}$| $0\frac{mm}{d} =< ET_{Stat} < 200\frac{mm}{d}$ |

For the evapotranspiration there are two rules that need to be fulfilled to be unplausible. One relative and one nominal. This is because, low measurement values tend to have high relative differences and would then get deleted too often.

To consider the possible inversion weather phenomena, stations higher than 800 m.a.S. are only tested against the lower limit in winter months (October-March).

Furthermore there are maximum and minimum treshold values to which the ET and T values are compared to. Those treshold values are set to reasonable values for germany.

### Precipitation

The precipitation measurements must pass through multiple quality checks.

#### daily sum is zero

As some precipitation station (e.g. Feldberg station) have measurements of 0mm in the 10 minutes dataset even though the daily dataset has measurements. This is especially true for the measurement points in 20th and early 21th century. This is probably the result of new measurement equipment to measure the 10 minutes values, but without a good quality control. back then the daily values are often measured with other equipments and have a better quality check they are going through, so that the values are more reliable.

To filter those wrongly measurements of 0 mm out of the data, the data is compared to the daily values from the DWD at the same location. For this reason the daily measurements are first filled up (see next chapter). <br>
The 10 minutes measurements get aggregated to daily values. If this daily sum is 0 mm, even though the daily data from the DWD is not 0, all the 10 minutes measurements of that day are deleted. Furthermore if the daily measurement is more than twice the aggregated 10 minutes values, all the 10 minutes data of that day are removed.

This check is the only reason why the daily precipitation values were downloaded in the first place.

#### consecutive equal values

Sometimes there are several consecutive 10 minutes values that are exactly the same. As the accuracy of the measurement is 0.01 mm, this is very improbable to be a real measurement and is more likely the result of splitting e.g. an hourly value into 6 values.

It is assumed, that filling the measurements up with values from the neighbor stations is more accurate than this dissemination. Therefor 3 consecutive same measurements are deleted, if their "Qualitätsnorm" from the DWD is not 3 (meaning that the measurements didn't get a good quality control from the DWD).

### treshold

Sometimes there are very high measurement values in the DWD data. Therefor a treshold of $50 mm / 10 min$ is applied. This treshold is higher then any KOSTRA 2020 precipitation with a return period of 100 years, which are around $30 mm / 10 min$. Values above this treshold are considered as erroneous.

Furthermore negativ values are filtered.

### single peaks

Sometimes there are very high single peaks in the DWD raw data. As high precipitations are highly unprobable to happen only in one 10 minute range, those data points are regarded as faulty. More precisely data points higher than $5 mm / 10 min$ where there was no rain before and after the measurement is removed.

## gap filling

To have complete timeseries, the gaps in the quality checked timeseries are filled with data from the neighbor stations. The neighboring stations are selected in the order of horizontal difference for the precipitation stations and in order of the elevation weighted distance (see chapter quality check - Temperature and Evapotranspiration) for T and ET stations. This is done by regionalising the neighbors measurements value to the station that is gap filled. Starting with the nearest neighbor station all available stations are taken until the timeserie is completely filled.

For the reginalisation, the multi-annual values for every station for the climate period of 1991-2020 are computed from the corresponding DWD grid.

**Table 3: The raster grids that are the basis for the regionalisation**
| parameter |  <div style="text-align: center">source</div> |
|:---:|---|
| Precipitation | DWD Climate Data Center (CDC): <br>HYRAS grids of multi-annual precipitation, period 1991-2020, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/grids_germany/multi_annual/hyras_de/precipitation/)|
| Temperature | DWD Climate Data Center (CDC): <br>Multi-annual means of grids of air temperature (2m) over Germany, period 1991-2020, version v1.0. <br>[online available](https://opendata.dwd.de/climate_environment/CDC/grids_germany/multi_annual/air_temperature_mean)|
| potential Evapotranspiration | DWD Climate Data Center (CDC): <br>Multi-annual grids of potential evapotranspiration over grass, 0.x, period 1991-2020, <br>[online available](https://opendata.dwd.de/climate_environment/CDC/grids_germany/multi_annual/evapo_p)|

As those grids have a coarse resolution with 1 km<sup>2</sup>, they got refined to a resolution of 25 meters, on the basis of a DEM25 (Copernicus). To refine the rasters the 1 km<sup>2</sup> DEM that was used by the DWD, was used. Together with the multi-annual raster value of the neighbor cells a linear regression is defined for every cell. The size of the window to produce the linear regression depends on the topology. Starting with a 5 x 5 km window, the standard deviation of the topology is computed. If this is smaller than 4 meters, than the window is increased by 1 km to each side. This step is repeated until the standard deviation is greater than 4 meters or the size of the window is greater than 13 x 13 km. This regression is then used on the DEM25 cells inside the 1 km<sup>2</sup> cell, to calculate the new multi-annual values.

Then to get a regionalisation factor the multi-annual values of both stations are compared. For T and ET only the yearly mean is taken into account. For the precipitation one winter(October-March) and one summer (April-September) factor is computed. The following equation explain the calculation of the filling values for the different parameters, based on their multi-annual mean(ma).

$T_{fillup} = T_{neighbor} + (T_{station,ma}-T_{neighbor,ma})$

$ET_{fillup} = ET_{neighbor} * \dfrac{ET_{station,ma}}{ET_{neighbor,ma}}$

$N_{fillup} = \begin{cases}
N_{neighbor} * \dfrac{N_{station,ma,winter}}{N_{neighbor,ma,winter}} \space if\space month\in[4:9]\\ 
N_{neighbor} * \dfrac{N_{station,ma,summer}}{N_{neighbor,ma,summer}} \space if\space month\notin[4:9]
\end{cases}$

For the precipitation and evapotranpiration stations only the closest station with quality checked data is taken to fill missing values. For the temperature stations the median of the regionalised values from the 5 closest stations (but not more than 100 km away)  to fill missing values.

### adjuting precipitation to daily station measurements

For the precipitation values the 10 minutes values are furthermore adjusted to the daily measurements. Therefor the daily sum is computed from the 10 minutes data. Then the quotient with the daily measurement is calculated and multiplied to every 10 minute measurement. So the difference to the daily measurement is added relatively to the measured value. In the end the gap filled 10 minutes precipitation values sum up to the same daily values as the daily values from the DWD.

But to prevent creating high peaks, there is an additional routine. If e.g. the neighbooring station has only one 10 minute measurement with rain, but the daily station measurement is high (e.g. $50mm/day$), this scaling routine would create a very high 10 minutes measurement ( in this example $50mm/10min$). To prevent this, the scalling can add at maximum $10mm/10min$ and is not allowed to create single peaks with more than $5mm/10min$ (see chapter about single peaks in the quality control chapter). If the scaling would exceed those tresholds, it is cutten to those tresholds, which has the side effect, that the daily measurement of that day isn't matching the 10 minute measurements anymore. But this is seen as the best compromise.

## Richter correction

This step is only done for the 10 minutes precipitation values. Here the filled precipitation values, get corrected like defined in Richter (1995).  

First of all, the horizon angle ("Horizontabschirmung") is calculated from a DGM25 and if the DGM25 was out of bound also from a DGM80. The DGM80 is bigger than the german border and therefor for stations around the border this is gives better results than the DGM20 which is only for the german territory. Therefore the rasters are sampled for their values on one single line of 75km, starting from the station. Then the angle to every point from the station is calculated. The Point with the biggest angle is taken as horizon angle for this line. This step is repeated for several lines ranging from north to south in 3° steps. Afterwards the Richter horizon angle is computed as:

$H’=0,15*H_{S-SW} + 0,35*H_{SW-W} +0,35*H_{W-NW} +0, 15*H_{NW-N}$

With this horizon angle the Richter exposition class is defined for every station.

Afterwards the daily correction is calculated with the following table and equation, based on the filled daily temperature at the station.

$\Delta N = b * N^E$

**Table 4: The Richter correction coeeficients. (Richter (1995) S. 67)**	
|precipitation typ|temperature|E|b<br>no-protection|b<br>little-protection|b<br>protected|b<br>heavy-protection|
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
|precip_sommer|>= 3 °C | 0,38 | 0,345 | 0,31 | 0,28 | 0,245 |
|precip_winter| >= 3 °C | 0,46 | 0,34 | 0,28 | 0,24 | 0,19 |
|mix|-0,7 °C < T < 3 °C | 0,55 | 0,535 | 0,39 | 0,305 | 0,185 |
|snow|<= -0,7°C|0,82 | 0,72 | 0,51 | 0,33 | 0,21 |

The daily correction ($\Delta N$) is then distributed to every 10 minute measurement where precipitation was measured. So the daily correction is applied as a block to the 10 minutes values. This results in relatively high corrections, when there was only little precipitation and relatively low corrections when the measured precipitation was high. As the systematic error is mostly due to wind and has therefor more effect, when there is low precipitation, this approach is better than adding it relatively to the measurement.

<br>

-------------------------

## Sources

- Richter, D. 1995. Ergebnisse methodischer Untersuchungen zur Korrektur des systematischen Meßfehlers des Hellmann-Niederschlagsmessers. Offenbach am Main: Selbstverl. des Dt. Wetterdienstes.
- Coperniicus. 2016. European Digital Elevation Model (EU-DEM), version 1.1. [online available](https://land.copernicus.eu/imagery-in-situ/eu-dem/eu-dem-v1.1)
- Stoelzle, Michael & Weiler, Markus & Steinbrich, Andreas. (2016) Starkregengefährdung in Baden-Württemberg – von der Methodenentwicklung zur Starkregenkartierung. Tag der Hydrologie.
- LARSIM Dokumentation, Stand 06.04.2023, online unter https://www.larsim.info/dokumentation/LARSIM-Dokumentation.pdf