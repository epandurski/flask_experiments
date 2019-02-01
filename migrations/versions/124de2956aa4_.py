"""empty message

Revision ID: 124de2956aa4
Revises: 28479f0d5dec
Create Date: 2019-02-01 18:39:02.826177

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '124de2956aa4'
down_revision = '28479f0d5dec'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('operator_debtor_id_fkey', 'operator', type_='foreignkey')
    op.create_foreign_key(None, 'operator', 'branch', ['debtor_id', 'branch_id'], ['debtor_id', 'branch_id'])
    op.drop_constraint('operator_transaction_debtor_id_fkey', 'operator_transaction', type_='foreignkey')
    op.create_foreign_key(None, 'operator_transaction', 'operator', ['debtor_id', 'operator_branch_id', 'operator_user_id'], ['debtor_id', 'branch_id', 'user_id'])
    op.drop_constraint('operator_transaction_request_debtor_id_fkey', 'operator_transaction_request', type_='foreignkey')
    op.create_foreign_key(None, 'operator_transaction_request', 'operator', ['debtor_id', 'operator_branch_id', 'operator_user_id'], ['debtor_id', 'branch_id', 'user_id'])
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'operator_transaction_request', type_='foreignkey')
    op.create_foreign_key('operator_transaction_request_debtor_id_fkey', 'operator_transaction_request', 'operator', ['debtor_id', 'operator_branch_id', 'operator_user_id'], ['debtor_id', 'branch_id', 'user_id'], ondelete='CASCADE')
    op.drop_constraint(None, 'operator_transaction', type_='foreignkey')
    op.create_foreign_key('operator_transaction_debtor_id_fkey', 'operator_transaction', 'operator', ['debtor_id', 'operator_branch_id', 'operator_user_id'], ['debtor_id', 'branch_id', 'user_id'], ondelete='CASCADE')
    op.drop_constraint(None, 'operator', type_='foreignkey')
    op.create_foreign_key('operator_debtor_id_fkey', 'operator', 'branch', ['debtor_id', 'branch_id'], ['debtor_id', 'branch_id'], ondelete='CASCADE')
    # ### end Alembic commands ###
