"""empty message

Revision ID: d4ba487420ab
Revises: ec245fb44c7c
Create Date: 2019-02-02 11:39:01.950857

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd4ba487420ab'
down_revision = 'ec245fb44c7c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('prepared_transfer', 'transfer_type',
               existing_type=sa.SMALLINT(),
               comment='1 -- circular transfer, 2 -- operator transaction, 3 -- direct transfer',
               existing_comment='1 -- circular transfer, 2 -- withdrawal, 3 -- deposit',
               existing_nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('prepared_transfer', 'transfer_type',
               existing_type=sa.SMALLINT(),
               comment='1 -- circular transfer, 2 -- withdrawal, 3 -- deposit',
               existing_comment='1 -- circular transfer, 2 -- operator transaction, 3 -- direct transfer',
               existing_nullable=False)
    # ### end Alembic commands ###
