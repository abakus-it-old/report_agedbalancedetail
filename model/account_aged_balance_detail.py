# -*- coding: utf-8 -*-

import time
from openerp import models
from openerp.report import report_sxw
from common_report_header import common_report_header

class aged_trial_detail_report(report_sxw.rml_parse, common_report_header):

    def __init__(self, cr, uid, name, context):
        super(aged_trial_detail_report, self).__init__(cr, uid, name, context=context)
        self.total_account = []
        self.localcontext.update({
            'time': time,
            'get_lines': self._get_lines,           
            'get_lines_with_out_partner': self._get_lines_with_out_partner,
            'get_total': self._get_total,
            'get_direction': self._get_direction,
            'get_for_period': self._get_for_period,
            'get_company': self._get_company,
            'get_currency': self._get_currency,
            'get_partners':self._get_partners,
            'get_target_move': self._get_target_move,
        })

    def set_context(self, objects, data, ids, report_type=None):
        obj_move = self.pool.get('account.move.line')
        ctx = data['form'].get('used_context', {})
        ctx.update({'fiscalyear': False, 'all_fiscalyear': True})
        self.query = '1=1'#obj_move._query_get(self.cr, self.uid)[0]
        self.direction_selection = data['form'].get('direction_selection', 'past')
        self.target_move = data['form'].get('target_move', 'all')
        self.date_from = data['form'].get('date_from', time.strftime('%Y-%m-%d'))
        if (data['form']['result_selection'] == 'customer' ):
            self.ACCOUNT_TYPE = ['receivable']
        elif (data['form']['result_selection'] == 'supplier'):
            self.ACCOUNT_TYPE = ['payable']
        else:
            self.ACCOUNT_TYPE = ['payable','receivable']
        return super(aged_trial_detail_report, self).set_context(objects, data, ids, report_type=report_type)

    def _get_lines(self, form):
        res = []
        company_id = form['company_id'][0]
        move_state = ['draft','posted']
        if self.target_move == 'posted':
            move_state = ['posted']
        self.cr.execute('SELECT DISTINCT res_partner.id AS id,\
                    res_partner.name AS name \
                FROM res_partner,account_move_line AS l, account_account, account_move am\
                WHERE (l.account_id=account_account.id) \
                    AND (l.move_id=am.id) \
                    AND (am.state IN %s)\
                    AND (account_account.type IN %s)\
                    AND account_account.active\
                    AND ((reconcile_id IS NULL)\
                       OR (reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                    AND (l.partner_id=res_partner.id)\
                    AND (l.date <= %s)\
                    AND ' + self.query + ' \
                ORDER BY res_partner.name', (tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, self.date_from,))
        partners = self.cr.dictfetchall()
        
        ## mise a 0 du total
        for i in range(7):
            self.total_account.append(0)
        #
        # Build a string like (1,2,3) for easy use in SQL query
        partner_ids = [x['id'] for x in partners]
        if not partner_ids:
            return []
        # This dictionary will store the debit-credit for all partners, using partner_id as key.

        totals = {}
        self.cr.execute('SELECT l.partner_id, SUM(l.debit-l.credit) \
                    FROM account_move_line AS l, account_account, account_move am \
                    WHERE (l.account_id = account_account.id) AND (l.move_id=am.id) \
                    AND (l.company_id = %s) \
                    AND (am.state IN %s)\
                    AND (account_account.type IN %s)\
                    AND (l.partner_id IN %s)\
                    AND ((l.reconcile_id IS NULL)\
                    OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                    AND ' + self.query + '\
                    AND account_account.active\
                    AND (l.date <= %s)\
                    GROUP BY l.partner_id ', (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), tuple(partner_ids), self.date_from, self.date_from,))
        t = self.cr.fetchall()
        for i in t:
            totals[i[0]] = i[1]

            
        # Use one query per period and store results in history (a list variable)
        # Each history will contain: history[1] = {'<partner_id>': <partner_debit-credit>}
        history = []
        for i in range(5):
            args_list = (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), tuple(partner_ids),self.date_from,)
            dates_query = '(COALESCE(l.date_maturity,l.date)'
            if form[str(i)]['start'] and form[str(i)]['stop']:
                dates_query += ' BETWEEN %s AND %s)'
                args_list += (form[str(i)]['start'], form[str(i)]['stop'])
            elif form[str(i)]['start']:
                dates_query += ' > %s)'
                args_list += (form[str(i)]['start'],)
            else:
                dates_query += ' < %s)'
                args_list += (form[str(i)]['stop'],)
            args_list += (self.date_from,)
            
            #MODIFIED            
            
            self.cr.execute('''SELECT l.partner_id, SUM(l.debit-l.credit), l.reconcile_partial_id, l.date_maturity, l.ref, j.code, am.name, acc.code 
                    FROM account_move_line l
                    LEFT JOIN account_journal j ON (l.journal_id = j.id)
                    LEFT JOIN account_account acc ON (l.account_id = acc.id)
                    LEFT JOIN res_currency c ON (l.currency_id=c.id)
                    LEFT JOIN account_move am ON (am.id=l.move_id)
                    WHERE (l.account_id = acc.id) AND (l.move_id=am.id)
                        AND (l.company_id = %s) \
                        AND (am.state IN %s)
                        AND (acc.type IN %s)
                        AND (l.partner_id IN %s)
                        AND ((l.reconcile_id IS NULL)
                          OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))
                        AND ''' + self.query + '''
                        AND acc.active
                        AND ''' + dates_query + '''
                    AND (l.date <= %s)
                    GROUP BY l.partner_id, l.reconcile_partial_id, l.date_maturity, l.ref, j.code, am.name, acc.code''', args_list)
            partners_partial = self.cr.fetchall()

            partners_amount = dict((i[0],0) for i in partners_partial)
            for partner_info in partners_partial:
                amount = 0
                if partner_info[2]:
                    # in case of partial reconciliation, we want to keep the left amount in the oldest period
                    self.cr.execute('''SELECT MIN(COALESCE(date_maturity,date)) FROM account_move_line WHERE reconcile_partial_id = %s''', (partner_info[2],))
                    date = self.cr.fetchall()
                    if date and args_list[-3] <= date[0][0] <= args_list[-2]:
                        # partial reconcilation
                        self.cr.execute('''SELECT SUM(l.debit-l.credit)
                                           FROM account_move_line AS l
                                           WHERE l.reconcile_partial_id = %s''', (partner_info[2],))
                        unreconciled_amount = self.cr.fetchall()
                        amount = unreconciled_amount[0][0]
                        partners_amount[partner_info[0]] += amount
                        
                else:
                    amount = partner_info[1]
                    partners_amount[partner_info[0]] += amount
                
                #MODIFIED
                if not partners_amount.has_key(str(partner_info[0])+'dict'):
                    partners_amount[str(partner_info[0])+'dict'] = []
                
                move_line_info = {'due_date': partner_info[3], 'journal': partner_info[5], 'ref': partner_info[4], 'account': partner_info[7], 'entry_label': partner_info[6], 'amount': amount}
                partners_amount[str(partner_info[0])+'dict'].append(move_line_info)

            history.append(partners_amount)

            
        # This dictionary will store the future or past of all partners
        future_past = {}
        if self.direction_selection == 'future':
            self.cr.execute('SELECT l.partner_id, SUM(l.debit-l.credit), l.date_maturity, l.ref, j.code, am.name, acc.code \
                            FROM account_move_line l \
                            LEFT JOIN account_journal j ON (l.journal_id = j.id) \
                            LEFT JOIN account_account acc ON (l.account_id = acc.id) \
                            LEFT JOIN res_currency c ON (l.currency_id=c.id) \
                            LEFT JOIN account_move am ON (am.id=l.move_id) \
                            WHERE (l.account_id=acc.id) AND (l.move_id=am.id) \
                                AND (l.company_id = %s) \
                                AND (am.state IN %s)\
                                AND (acc.type IN %s)\
                                AND (COALESCE(l.date_maturity, l.date) < %s)\
                                AND (l.partner_id IN %s)\
                                AND ((l.reconcile_id IS NULL)\
                                OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                                AND '+ self.query + '\
                                AND acc.active\
                                AND (l.date <= %s)\
                            GROUP BY l.partner_id, l.date_maturity, l.ref, j.code, am.name, acc.code', (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, tuple(partner_ids),self.date_from, self.date_from,))
            t = self.cr.fetchall()
            
            line_dict = {}
            for i in t:
                if not future_past.has_key(i[0]):
                    future_past[i[0]] = 0
                future_past[i[0]] += i[1]
                if not line_dict.has_key(str(i[0])+'dict'):
                    line_dict[str(i[0])+'dict'] = []
                
                move_line_info = {'due_date': i[2], 'journal': i[4], 'ref': i[3], 'account': i[6], 'entry_label': i[5], 'amount': i[1]}
                line_dict[str(i[0])+'dict'].append(move_line_info)

            history.append(line_dict)
                
        elif self.direction_selection == 'past': # Using elif so people could extend without this breaking
            self.cr.execute('SELECT l.partner_id, SUM(l.debit-l.credit), l.date_maturity, l.ref, j.code, am.name, acc.code \
                            FROM account_move_line l \
                            LEFT JOIN account_journal j ON (l.journal_id = j.id) \
                            LEFT JOIN account_account acc ON (l.account_id = acc.id) \
                            LEFT JOIN res_currency c ON (l.currency_id=c.id) \
                            LEFT JOIN account_move am ON (am.id=l.move_id) \
                            WHERE (l.account_id=acc.id) AND (l.move_id=am.id)\
                                AND (l.company_id = %s) \
                                AND (am.state IN %s)\
                                AND (acc.type IN %s)\
                                AND (COALESCE(l.date_maturity,l.date) > %s)\
                                AND (l.partner_id IN %s)\
                                AND ((l.reconcile_id IS NULL)\
                                OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                                AND '+ self.query + '\
                                AND acc.active\
                                AND (l.date <= %s)\
                                GROUP BY l.partner_id, l.date_maturity, l.ref, j.code, am.name, acc.code', (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, tuple(partner_ids), self.date_from, self.date_from,))
            t = self.cr.fetchall()
            
            line_dict = {}
            for i in t:
                if not future_past.has_key(i[0]):
                    future_past[i[0]] = 0
                future_past[i[0]] += i[1]
                if not line_dict.has_key(str(i[0])+'dict'):
                    line_dict[str(i[0])+'dict'] = []
                
                move_line_info = {'due_date': i[2], 'journal': i[4], 'ref': i[3], 'account': i[6], 'entry_label': i[5], 'amount': i[1]}
                line_dict[str(i[0])+'dict'].append(move_line_info)

            history.append(line_dict)
            
        for partner in partners:
            values = {}
            ## If choise selection is in the future
            if self.direction_selection == 'future':
                # Query here is replaced by one query which gets the all the partners their 'before' value
                before = False
                if future_past.has_key(partner['id']):
                    before = [ future_past[partner['id']] ]
                self.total_account[6] = self.total_account[6] + (before and before[0] or 0.0)
                values['direction'] = before and before[0] or 0.0
            elif self.direction_selection == 'past': # Changed this so people could in the future create new direction_selections
                # Query here is replaced by one query which gets the all the partners their 'after' value
                after = False
                if future_past.has_key(partner['id']): # Making sure this partner actually was found by the query
                    after = [ future_past[partner['id']] ]

                self.total_account[6] = self.total_account[6] + (after and after[0] or 0.0)
                values['direction'] = after and after[0] or 0.0
            
            #NEW START
            lines = []
            for i in range(6):
                if history[i].has_key(str(partner['id'])+'dict'):
                    for line in history[i][str(partner['id'])+'dict']:
                        if line['amount'] and line['amount'] != 0:
                            line['name'] = '%s|%s|%s|%s|%s'%(line['due_date'],line['journal'],line['ref'],line['account'],line['entry_label'])
                            line[str(i)] = line['amount']
                            line['total'] = line['amount']
                            for j in range(6):
                                if j != i:
                                    line[str(j)] = 0.0
                            lines.insert( 0, line)
            values['lines'] = sorted(lines, key=lambda k: k['due_date'], reverse=True)
            #NEW END

            for i in range(5):
                during = False
                if history[i].has_key(partner['id']):
                    during = [ history[i][partner['id']] ]
                # Ajout du compteur
                self.total_account[(i)] = self.total_account[(i)] + (during and during[0] or 0)
                values[str(i)] = during and during[0] or 0.0
            
            total = False
            if totals.has_key( partner['id'] ):
                total = [ totals[partner['id']] ]
            values['total'] = total and total[0] or 0.0
            ## Add for total
            self.total_account[(i+1)] = self.total_account[(i+1)] + (total and total[0] or 0.0)
            values['name'] = partner['name']
            
            if values['total'] and values['total'] != 0:
                res.append(values)

        total = 0.0
        totals = {}
        for r in res:
            total += float(r['total'] or 0.0)
            for i in range(5)+['direction']:
                totals.setdefault(str(i), 0.0)
                totals[str(i)] += float(r[str(i)] or 0.0)        

        return res

    def _get_lines_with_out_partner(self, form):
        res = []
        company_id = form['company_id'][0]
        move_state = ['draft','posted']
        if self.target_move == 'posted':
            move_state = ['posted']

        ## mise a 0 du total
        for i in range(7):
            self.total_account.append(0)
        totals = {}
        self.cr.execute('SELECT SUM(l.debit-l.credit) \
                    FROM account_move_line AS l, account_account, account_move am \
                    WHERE (l.account_id = account_account.id) AND (l.move_id=am.id)\
                    AND (l.company_id = %s) \
                    AND (am.state IN %s)\
                    AND (l.partner_id IS NULL)\
                    AND (account_account.type IN %s)\
                    AND ((l.reconcile_id IS NULL) \
                    OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                    AND ' + self.query + '\
                    AND (l.date <= %s)\
                    AND account_account.active ',(company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, self.date_from,))
        t = self.cr.fetchall()
        for i in t:
            totals['Unknown Partner'] = i[0]
        future_past = {}
        if self.direction_selection == 'future':
            self.cr.execute('SELECT SUM(l.debit-l.credit) \
                        FROM account_move_line AS l, account_account, account_move am\
                        WHERE (l.account_id=account_account.id) AND (l.move_id=am.id)\
                        AND (l.company_id = %s) \
                        AND (am.state IN %s)\
                        AND (l.partner_id IS NULL)\
                        AND (account_account.type IN %s)\
                        AND (COALESCE(l.date_maturity, l.date) < %s)\
                        AND ((l.reconcile_id IS NULL)\
                        OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                        AND '+ self.query + '\
                        AND account_account.active ', (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, self.date_from))
            t = self.cr.fetchall()
            for i in t:
                future_past['Unknown Partner'] = i[0]
        elif self.direction_selection == 'past': # Using elif so people could extend without this breaking
            self.cr.execute('SELECT SUM(l.debit-l.credit) \
                    FROM account_move_line AS l, account_account, account_move am \
                    WHERE (l.account_id=account_account.id) AND (l.move_id=am.id)\
                        AND (l.company_id = %s) \
                        AND (am.state IN %s)\
                        AND (l.partner_id IS NULL)\
                        AND (account_account.type IN %s)\
                        AND (COALESCE(l.date_maturity,l.date) > %s)\
                        AND ((l.reconcile_id IS NULL)\
                        OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                        AND '+ self.query + '\
                        AND account_account.active ', (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from, self.date_from))
            t = self.cr.fetchall()
            for i in t:
                future_past['Unknown Partner'] = i[0]
        history = []

        for i in range(5):
            args_list = (company_id, tuple(move_state), tuple(self.ACCOUNT_TYPE), self.date_from,)
            dates_query = '(COALESCE(l.date_maturity,l.date)'
            if form[str(i)]['start'] and form[str(i)]['stop']:
                dates_query += ' BETWEEN %s AND %s)'
                args_list += (form[str(i)]['start'], form[str(i)]['stop'])
            elif form[str(i)]['start']:
                dates_query += ' > %s)'
                args_list += (form[str(i)]['start'],)
            else:
                dates_query += ' < %s)'
                args_list += (form[str(i)]['stop'],)
            args_list += (self.date_from,)
            self.cr.execute('SELECT SUM(l.debit-l.credit)\
                    FROM account_move_line AS l, account_account, account_move am \
                    WHERE (l.account_id = account_account.id) AND (l.move_id=am.id)\
                        AND (l.company_id = %s) \
                        AND (am.state IN %s)\
                        AND (account_account.type IN %s)\
                        AND (l.partner_id IS NULL)\
                        AND ((l.reconcile_id IS NULL)\
                        OR (l.reconcile_id IN (SELECT recon.id FROM account_move_reconcile AS recon WHERE recon.create_date > %s )))\
                        AND '+ self.query + '\
                        AND account_account.active\
                        AND ' + dates_query + '\
                    AND (l.date <= %s)\
                    GROUP BY l.partner_id', args_list)
            t = self.cr.fetchall()
            d = {}
            for i in t:
                d['Unknown Partner'] = i[0]
            history.append(d)

        values = {}
        if self.direction_selection == 'future':
            before = False
            if future_past.has_key('Unknown Partner'):
                before = [ future_past['Unknown Partner'] ]
            self.total_account[6] = self.total_account[6] + (before and before[0] or 0.0)
            values['direction'] = before and before[0] or 0.0
        elif self.direction_selection == 'past':
            after = False
            if future_past.has_key('Unknown Partner'):
                after = [ future_past['Unknown Partner'] ]
            self.total_account[6] = self.total_account[6] + (after and after[0] or 0.0)
            values['direction'] = after and after[0] or 0.0

        for i in range(5):
            during = False
            if history[i].has_key('Unknown Partner'):
                during = [ history[i]['Unknown Partner'] ]
            self.total_account[(i)] = self.total_account[(i)] + (during and during[0] or 0)
            values[str(i)] = during and during[0] or 0.0

        total = False
        if totals.has_key( 'Unknown Partner' ):
            total = [ totals['Unknown Partner'] ]
        values['total'] = total and total[0] or 0.0
        ## Add for total
        self.total_account[(i+1)] = self.total_account[(i+1)] + (total and total[0] or 0.0)
        values['name'] = 'Unknown Partner'

        if values['total'] and values['total'] != 0:
            res.append(values)

        total = 0.0
        totals = {}
        for r in res:
            total += float(r['total'] or 0.0)
            for i in range(5)+['direction']:
                totals.setdefault(str(i), 0.0)
                totals[str(i)] += float(r[str(i)] or 0.0)
        
        return res

    def _get_total(self,pos):
        period = self.total_account[int(pos)]
        return period or 0.0

    def _get_direction(self,pos):
        period = self.total_account[int(pos)]
        return period or 0.0

    def _get_for_period(self,pos):
        period = self.total_account[int(pos)]
        return period or 0.0

    def _get_partners(self,data):
        # TODO: deprecated, to remove in trunk
        if data['form']['result_selection'] == 'customer':
            return self._translate('Receivable Accounts')
        elif data['form']['result_selection'] == 'supplier':
            return self._translate('Payable Accounts')
        elif data['form']['result_selection'] == 'customer_supplier':
            return self._translate('Receivable and Payable Accounts')
        return ''


class report_agedbalancedetail(models.AbstractModel):
    _name = 'report.report_agedbalancedetail.report_agedbalancedetail'
    _inherit = 'report.abstract_report'
    _template = 'report_agedbalancedetail.report_agedbalancedetail'
    _wrapped_report_class = aged_trial_detail_report
