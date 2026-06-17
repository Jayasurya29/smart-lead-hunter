import api from './client'

/* ════════════════════════════════════════
   TYPES — mirror app/services/affiliations.py
   ════════════════════════════════════════ */

export interface AffiliationCoverageItem {
  account_type: 'existing_hotel' | 'potential_lead'
  account_id: number
  name: string | null
  via: 'explicit' | 'portfolio'
}

export interface PersonEmployer {
  type: string
  id?: number
  name: string | null
  scope: string | null
}

export interface PersonAffiliations {
  person_type: 'contact' | 'lead_contact'
  person_id: number
  employer: PersonEmployer | null
  employers: PersonEmployer[]
  is_portfolio_buyer: boolean
  coverage_count: number
  derived_portfolio_count: number
  coverage: AffiliationCoverageItem[]
  former_employers?: { name: string | null; account_type?: string; account_id?: number | null }[]
}

export interface AccountCoveragePerson {
  person_type: 'contact' | 'lead_contact'
  person_id: number
  name: string | null
  title: string | null
  organization: string | null
  email: string | null
  contact_category: string | null
  is_decision_maker: boolean | null
  is_saved: boolean
  scope: string | null
  via: string
  relationship: string
}

export interface AccountCoverage {
  account_type: string
  account_id: number
  management_company: string | null
  people_count: number
  people: AccountCoveragePerson[]
}

/* ════════════════════════════════════════
   API CALLS — cookie auth via the shared client
   ════════════════════════════════════════ */

export async function fetchPersonAffiliations(
  personType: 'contact' | 'lead_contact',
  personId: number,
): Promise<PersonAffiliations> {
  const { data } = await api.get<PersonAffiliations>(
    `/api/affiliations/person/${personType}/${personId}`,
  )
  return data
}

export async function fetchAccountCoverage(
  accountType: 'existing_hotel' | 'potential_lead',
  accountId: number,
): Promise<AccountCoverage> {
  const { data } = await api.get<AccountCoverage>(
    `/api/affiliations/account/${accountType}/${accountId}`,
  )
  return data
}

/** Verify (save) a lead contact by id — brings it into the trusted directory. */
export async function saveLeadContact(contactId: number): Promise<void> {
  await api.post(`/api/lead-contacts/${contactId}/save`)
}
