"""Initial database creation

Revision ID: V1.0.0
Revises:
Create Date: 2024-08-23 14:44:43.130167

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import geoalchemy2


# revision identifiers, used by Alembic.
revision: str = 'V1.0.0'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('droped_stations',
    sa.Column('station_id', sa.Integer(), nullable=False, comment='The station id that got droped'),
    sa.Column('para', sa.CHAR(length=3), nullable=False, comment='The parameter (n,t,et,n_d) of the station that got droped'),
    sa.Column('why', sa.Text(), nullable=False, comment='The reason why the station got droped'),
    sa.Column('timestamp', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=False, comment='The timestamp when the station got droped'),
    sa.PrimaryKeyConstraint('station_id', 'para'),
    comment='This table is there to save the station ids that got droped, so they wont GET recreated'
    )
    op.create_table('meta_et',
    sa.Column('station_id', sa.Integer(), nullable=False, comment='official DWD-ID of the station'),
    sa.Column('is_real', sa.Boolean(), nullable=False, comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station."),
    sa.Column('raw_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on own "raw" data is available'),
    sa.Column('raw_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('hist_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('filled_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on filled data is available'),
    sa.Column('filled_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when filled data is available'),
    sa.Column('last_imp_from', sa.TIMESTAMP(), nullable=True, comment='The minimal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_until', sa.TIMESTAMP(), nullable=True, comment='The maximal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_filled', sa.Boolean(), nullable=False, comment='Got the last import already filled?'),
    sa.Column('stationshoehe', sa.Integer(), nullable=False, comment='The stations height above the ground in meters'),
    sa.Column('stationsname', sa.CHAR(length=30), nullable=False, comment='The stations official name as text'),
    sa.Column('bundesland', sa.CHAR(length=30), nullable=False, comment='The state the station is located in'),
    sa.Column('geometry', geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the WGS84 coordinate reference system (EPSG:4326)'),
    sa.Column('geometry_utm', geoalchemy2.types.Geometry(geometry_type='POINT', srid=25832, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the UTM32 coordinate reference system (EPSG:25832)'),
    sa.Column('last_imp_qc', sa.Boolean(), nullable=False, comment='Got the last import already quality checked?'),
    sa.Column('qc_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on quality checked("qc") data is available'),
    sa.Column('qc_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when quality checked("qc") data is available'),
    sa.Column('qc_droped', sa.FLOAT(), nullable=False, comment='The percentage of droped values during the quality check'),
    sa.PrimaryKeyConstraint('station_id'),
    comment='The Meta informations of the evapotranspiration stations.'
    )
    op.create_index('idx_meta_et_geometry', 'meta_et', ['geometry'], unique=False, postgresql_using='gist')
    op.create_index('idx_meta_et_geometry_utm', 'meta_et', ['geometry_utm'], unique=False, postgresql_using='gist')
    op.create_table('meta_n',
    sa.Column('last_imp_corr', sa.Boolean(), nullable=False, comment='Got the last import already Richter corrected?'),
    sa.Column('corr_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on corrected data is available'),
    sa.Column('corr_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when corrected data is available'),
    sa.Column('horizon', sa.FLOAT(), nullable=False, comment='The horizon angle in degrees, how it got defined by Richter(1995).'),
    sa.Column('richter_class', sa.String(), nullable=False, comment='The Richter exposition class, that got derived from the horizon angle.'),
    sa.Column('quot_filled_hyras', sa.FLOAT(), nullable=False, comment='The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean HYRAS value (1991-2020)'),
    sa.Column('quot_filled_regnie', sa.FLOAT(), nullable=False, comment='The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean REGNIE value (1991-2020)'),
    sa.Column('quot_filled_dwd_grid', sa.FLOAT(), nullable=False, comment='The quotient betwen the mean yearly value from the filled timeserie to the multi annual yearly mean DWD grid value (1991-2020)'),
    sa.Column('quot_corr_filled', sa.FLOAT(), nullable=False, comment='The quotient betwen the mean yearly value from the Richter corrected timeserie to the mean yearly value from the filled timeserie'),
    sa.Column('station_id', sa.Integer(), nullable=False, comment='official DWD-ID of the station'),
    sa.Column('is_real', sa.Boolean(), nullable=False, comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station."),
    sa.Column('raw_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on own "raw" data is available'),
    sa.Column('raw_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('hist_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('filled_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on filled data is available'),
    sa.Column('filled_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when filled data is available'),
    sa.Column('last_imp_from', sa.TIMESTAMP(), nullable=True, comment='The minimal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_until', sa.TIMESTAMP(), nullable=True, comment='The maximal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_filled', sa.Boolean(), nullable=False, comment='Got the last import already filled?'),
    sa.Column('stationshoehe', sa.Integer(), nullable=False, comment='The stations height above the ground in meters'),
    sa.Column('stationsname', sa.CHAR(length=30), nullable=False, comment='The stations official name as text'),
    sa.Column('bundesland', sa.CHAR(length=30), nullable=False, comment='The state the station is located in'),
    sa.Column('geometry', geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the WGS84 coordinate reference system (EPSG:4326)'),
    sa.Column('geometry_utm', geoalchemy2.types.Geometry(geometry_type='POINT', srid=25832, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the UTM32 coordinate reference system (EPSG:25832)'),
    sa.Column('last_imp_qc', sa.Boolean(), nullable=False, comment='Got the last import already quality checked?'),
    sa.Column('qc_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on quality checked("qc") data is available'),
    sa.Column('qc_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when quality checked("qc") data is available'),
    sa.Column('qc_droped', sa.FLOAT(), nullable=False, comment='The percentage of droped values during the quality check'),
    sa.PrimaryKeyConstraint('station_id'),
    comment='The Meta informations of the precipitation stations.'
    )
    op.create_index('idx_meta_n_geometry', 'meta_n', ['geometry'], unique=False, postgresql_using='gist')
    op.create_index('idx_meta_n_geometry_utm', 'meta_n', ['geometry_utm'], unique=False, postgresql_using='gist')
    op.create_table('meta_n_d',
    sa.Column('station_id', sa.Integer(), nullable=False, comment='official DWD-ID of the station'),
    sa.Column('is_real', sa.Boolean(), nullable=False, comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station."),
    sa.Column('raw_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on own "raw" data is available'),
    sa.Column('raw_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('hist_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('filled_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on filled data is available'),
    sa.Column('filled_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when filled data is available'),
    sa.Column('last_imp_from', sa.TIMESTAMP(), nullable=True, comment='The minimal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_until', sa.TIMESTAMP(), nullable=True, comment='The maximal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_filled', sa.Boolean(), nullable=False, comment='Got the last import already filled?'),
    sa.Column('stationshoehe', sa.Integer(), nullable=False, comment='The stations height above the ground in meters'),
    sa.Column('stationsname', sa.CHAR(length=30), nullable=False, comment='The stations official name as text'),
    sa.Column('bundesland', sa.CHAR(length=30), nullable=False, comment='The state the station is located in'),
    sa.Column('geometry', geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the WGS84 coordinate reference system (EPSG:4326)'),
    sa.Column('geometry_utm', geoalchemy2.types.Geometry(geometry_type='POINT', srid=25832, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the UTM32 coordinate reference system (EPSG:25832)'),
    sa.PrimaryKeyConstraint('station_id'),
    comment='The Meta informations of the daily precipitation stations.'
    )
    op.create_index('idx_meta_n_d_geometry', 'meta_n_d', ['geometry'], unique=False, postgresql_using='gist')
    op.create_index('idx_meta_n_d_geometry_utm', 'meta_n_d', ['geometry_utm'], unique=False, postgresql_using='gist')
    op.create_table('meta_t',
    sa.Column('station_id', sa.Integer(), nullable=False, comment='official DWD-ID of the station'),
    sa.Column('is_real', sa.Boolean(), nullable=False, comment=" 'Is this station a real station with own measurements or only a virtual station, to have complete timeseries for every precipitation station."),
    sa.Column('raw_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on own "raw" data is available'),
    sa.Column('raw_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('hist_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when own "raw" data is available'),
    sa.Column('filled_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on filled data is available'),
    sa.Column('filled_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when filled data is available'),
    sa.Column('last_imp_from', sa.TIMESTAMP(), nullable=True, comment='The minimal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_until', sa.TIMESTAMP(), nullable=True, comment='The maximal timestamp of the last import, that might not yet have been treated'),
    sa.Column('last_imp_filled', sa.Boolean(), nullable=False, comment='Got the last import already filled?'),
    sa.Column('stationshoehe', sa.Integer(), nullable=False, comment='The stations height above the ground in meters'),
    sa.Column('stationsname', sa.CHAR(length=30), nullable=False, comment='The stations official name as text'),
    sa.Column('bundesland', sa.CHAR(length=30), nullable=False, comment='The state the station is located in'),
    sa.Column('geometry', geoalchemy2.types.Geometry(geometry_type='POINT', srid=4326, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the WGS84 coordinate reference system (EPSG:4326)'),
    sa.Column('geometry_utm', geoalchemy2.types.Geometry(geometry_type='POINT', srid=25832, from_text='ST_GeomFromEWKT', name='geometry', nullable=False), nullable=False, comment='The stations location in the UTM32 coordinate reference system (EPSG:25832)'),
    sa.Column('last_imp_qc', sa.Boolean(), nullable=False, comment='Got the last import already quality checked?'),
    sa.Column('qc_from', sa.TIMESTAMP(), nullable=True, comment='The timestamp from when on quality checked("qc") data is available'),
    sa.Column('qc_until', sa.TIMESTAMP(), nullable=True, comment='The timestamp until when quality checked("qc") data is available'),
    sa.Column('qc_droped', sa.FLOAT(), nullable=False, comment='The percentage of droped values during the quality check'),
    sa.PrimaryKeyConstraint('station_id'),
    comment='The Meta informations of the temperature stations.'
    )
    op.create_index('idx_meta_t_geometry', 'meta_t', ['geometry'], unique=False, postgresql_using='gist')
    op.create_index('idx_meta_t_geometry_utm', 'meta_t', ['geometry_utm'], unique=False, postgresql_using='gist')
    op.create_table('needed_download_time',
    sa.Column('timestamp', sa.TIMESTAMP(), server_default=sa.text('now()'), nullable=False, comment='The timestamp when the download hapend.'),
    sa.Column('quantity', sa.Integer(), nullable=False, comment='The number of stations that got downloaded'),
    sa.Column('aggregate', sa.String(), nullable=False, comment='The chosen aggregation. e.g. hourly, 10min, daily, ...'),
    sa.Column('timespan', sa.Interval(), nullable=False, comment='The timespan of the downloaded timeseries. e.g. 2 years'),
    sa.Column('zip', sa.Boolean(), nullable=False, comment='Was the download zipped?'),
    sa.Column('pc', sa.String(), nullable=False, comment='The name of the pc that downloaded the timeseries.'),
    sa.Column('duration', sa.Interval(), nullable=False, comment='The needed time to download and create the timeserie'),
    sa.Column('output_size', sa.Integer(), nullable=False, comment='The size of the created output file in bytes'),
    sa.PrimaryKeyConstraint('timestamp'),
    comment='Saves the time needed to save the timeseries. This helps predicting download time'
    )
    op.create_table('para_variables',
    sa.Column('para', sa.String(length=3), nullable=False, comment='The parameter for which the variables are valid. e.g. n/n_d/t/et.'),
    sa.Column('start_tstp_last_imp', sa.TIMESTAMP(), nullable=True, comment='At what timestamp did the last complete import start. This is then the maximum timestamp for which to expand the timeseries to.'),
    sa.Column('max_tstp_last_imp', sa.TIMESTAMP(), nullable=True, comment='The maximal timestamp of the last imports raw data of all the timeseries'),
    sa.PrimaryKeyConstraint('para'),
    comment='This table is there to save specific variables that are nescesary for the functioning of the scripts'
    )
    op.create_table('raw_files',
    sa.Column('para', sa.String(), nullable=False, comment='The parameter that got downloaded for this file. e.g. t, et, n_d, n'),
    sa.Column('filepath', sa.String(), nullable=False, comment='The filepath on the CDC Server'),
    sa.Column('modtime', sa.TIMESTAMP(), nullable=False, comment='The modification time on the CDC Server of the coresponding file'),
    sa.PrimaryKeyConstraint('para', 'filepath'),
    comment='The files that got imported from the CDC Server.'
    )
    op.create_table('richter_values',
    sa.Column('precipitation_typ', sa.Text(), nullable=False, comment="The type of precipitation. e.g. 'Schnee', 'Regen', ..."),
    sa.Column('temperaturbereich', sa.Text(), nullable=False, comment='The temperature range.'),
    sa.Column('e', sa.FLOAT(), nullable=False, comment='The e-value of the equation.'),
    sa.Column('b_no-protection', sa.FLOAT(), nullable=False, comment="The b-value of the equation for exposition class 'no protection'."),
    sa.Column('b_little-protection', sa.FLOAT(), nullable=False, comment="The b-value of the equation for exposition class 'little protection'."),
    sa.Column('b_protected', sa.FLOAT(), nullable=False, comment="The b-value of the equation for exposition class 'protected'."),
    sa.Column('b_heavy-protection', sa.FLOAT(), nullable=False, comment="The b-value of the equation for exposition class 'heavy protection'."),
    sa.PrimaryKeyConstraint('precipitation_typ'),
    comment='The Richter values for the equation.'
    )
    op.create_table('settings',
    sa.Column('key', sa.String(length=20), nullable=False, comment='The key of the setting'),
    sa.Column('value', sa.String(length=20), nullable=False, comment='The value of the setting'),
    sa.PrimaryKeyConstraint('key'),
    comment='This table saves settings values for the script-databse connection. E.G. the latest package version that updated the database.'
    )
    op.create_table('stations_raster_values',
    sa.Column('station_id', sa.Integer(), nullable=False, comment='The DWD-ID of the station.'),
    sa.Column('raster_key', sa.String(), nullable=False, comment="The name of the raster. e.g. 'dwd' or 'hyras'"),
    sa.Column('parameter', sa.String(), nullable=False, comment="The parameter of the raster. e.g. 'p_wihj', 'p_sohj', 'p_year', 't_year', 'et_year'"),
    sa.Column('value', sa.Integer(), nullable=False, comment='The value of the raster for the station.'),
    sa.Column('distance', sa.FLOAT(), nullable=False, comment='The distance of the station to the raster value in meters.'),
    sa.PrimaryKeyConstraint('station_id', 'raster_key', 'parameter'),
    comment='The multi annual climate raster values for each station.'
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('stations_raster_values')
    op.drop_table('settings')
    op.drop_table('richter_values')
    op.drop_table('raw_files')
    op.drop_table('para_variables')
    op.drop_table('needed_download_time')
    op.drop_index('idx_meta_t_geometry_utm', table_name='meta_t', postgresql_using='gist')
    op.drop_index('idx_meta_t_geometry', table_name='meta_t', postgresql_using='gist')
    op.drop_table('meta_t')
    op.drop_index('idx_meta_n_d_geometry_utm', table_name='meta_n_d', postgresql_using='gist')
    op.drop_index('idx_meta_n_d_geometry', table_name='meta_n_d', postgresql_using='gist')
    op.drop_table('meta_n_d')
    op.drop_index('idx_meta_n_geometry_utm', table_name='meta_n', postgresql_using='gist')
    op.drop_index('idx_meta_n_geometry', table_name='meta_n', postgresql_using='gist')
    op.drop_table('meta_n')
    op.drop_index('idx_meta_et_geometry_utm', table_name='meta_et', postgresql_using='gist')
    op.drop_index('idx_meta_et_geometry', table_name='meta_et', postgresql_using='gist')
    op.drop_table('meta_et')
    op.drop_table('droped_stations')
    # ### end Alembic commands ###