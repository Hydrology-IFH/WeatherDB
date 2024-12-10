"""Remove station_ma_raster values because they were faulty.
rename droped to dropped

Revision ID: V1.0.5
Revises: V1.0.2
Create Date: 2024-12-06 14:33:30.129005

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'V1.0.5'
down_revision: Union[str, None] = 'V1.0.2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text(
        """
        DELETE FROM station_ma_raster
        WHERE True;
        """))

    op.rename_table('droped_stations', 'dropped_stations')
    op.alter_column('dropped_stations', 'station_id',
               existing_type=sa.INTEGER(),
               comment='The station id that got dropped',
               existing_comment='The station id that got droped',
               existing_nullable=False)
    op.alter_column('dropped_stations', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter (p,t,et,p_d) of the station that got dropped',
               existing_comment='The parameter (n,t,et,p_d) of the station that got droped',
               existing_nullable=False)
    op.alter_column('dropped_stations', 'why',
               existing_type=sa.TEXT(),
               comment='The reason why the station got dropped',
               existing_comment='The reason why the station got droped',
               existing_nullable=False)
    op.alter_column('dropped_stations', 'timestamp',
               existing_type=sa.TIMESTAMP(),
               comment='The timestamp when the station got dropped',
               existing_comment='The timestamp when the station got droped',
               existing_nullable=False,
               existing_server_default=sa.text('now()'))
    op.create_table_comment(
        'dropped_stations',
        'This table is there to save the station ids that got dropped, so they wont GET recreated',
        existing_comment='This table is there to save the station ids that got droped, so they wont GET recreated',
        schema=None
    )

    op.alter_column('parameter_variables', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter for which the variables are valid. e.g. p/p_d/t/et.',
               existing_comment='The parameter for which the variables are valid. e.g. n/p_d/t/et.',
               existing_nullable=False)

    op.alter_column('raw_files', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter that got downloaded for this file. e.g. t, et, p_d, p',
               existing_comment='The parameter that got downloaded for this file. e.g. t, et, p_d, n',
               existing_nullable=False)

    op.alter_column('meta_et', 'qc_droped',
                    new_column_name='qc_dropped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')
    op.alter_column('meta_p', 'qc_droped',
                    new_column_name='qc_dropped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')
    op.alter_column('meta_t', 'qc_droped',
                    new_column_name='qc_dropped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')

    op.execute(sa.text("DROP VIEW IF EXISTS station_ma_timeseries_quotient_view CASCADE;"))

def downgrade() -> None:
    op.execute(sa.text(
        """
        DELETE FROM station_ma_raster
        WHERE True;
        """))

    op.alter_column('raw_files', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter that got downloaded for this file. e.g. t, et, p_d, n',
               existing_comment='The parameter that got downloaded for this file. e.g. t, et, p_d, p',
               existing_nullable=False)

    op.alter_column('parameter_variables', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter for which the variables are valid. e.g. n/p_d/t/et.',
               existing_comment='The parameter for which the variables are valid. e.g. p/p_d/t/et.',
               existing_nullable=False)

    op.create_table_comment(
        'dropped_stations',
        'This table is there to save the station ids that got droped, so they wont GET recreated',
        existing_comment='This table is there to save the station ids that got dropped, so they wont GET recreated',
        schema=None
    )
    op.alter_column('dropped_stations', 'timestamp',
               existing_type=sa.TIMESTAMP(),
               comment='The timestamp when the station got droped',
               existing_comment='The timestamp when the station got dropped',
               existing_nullable=False,
               existing_server_default=sa.text('now()'))
    op.alter_column('dropped_stations', 'why',
               existing_type=sa.TEXT(),
               comment='The reason why the station got droped',
               existing_comment='The reason why the station got dropped',
               existing_nullable=False)
    op.alter_column('dropped_stations', 'parameter',
               existing_type=sa.VARCHAR(length=3),
               comment='The parameter (n,t,et,p_d) of the station that got droped',
               existing_comment='The parameter (p,t,et,p_d) of the station that got dropped',
               existing_nullable=False)
    op.alter_column('dropped_stations', 'station_id',
               existing_type=sa.INTEGER(),
               comment='The station id that got droped',
               existing_comment='The station id that got dropped',
               existing_nullable=False)
    op.rename_table('dropped_stations', 'droped_stations')

    op.alter_column('meta_t', 'qc_dropped',
                    new_column_name='qc_droped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')
    op.alter_column('meta_p', 'qc_dropped',
                    new_column_name='qc_droped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')
    op.alter_column('meta_et', 'qc_dropped',
                    new_column_name='qc_droped',
                    existing_type=sa.Float(),
                    nullable=True,
                    comment='The percentage of dropped values during the quality check')

    op.execute(sa.text("DROP VIEW IF EXISTS station_ma_timeseries_raster_quotient_view CASCADE;"))
