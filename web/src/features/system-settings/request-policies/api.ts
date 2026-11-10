import { api } from '@/lib/api'
import { requireServerSuccess } from '@/lib/server-error-message'

export type PolicyConfig = {
  options: Record<string, string>
}
export type PolicyDecision = { action: string; reason: string; source: string }
export type QueueSnapshot = {
  waiting: number
  running: number
  overloaded: boolean
  age_ms: number
}
export type PolicyEvent = {
  attempt: number
  channel_id?: number
  group?: string
  rule?: string
  status?: number
  error_code?: string
  error_source?: string
  elapsed_ms: number
  decision: PolicyDecision
  health?: string
  // Decision-time queue snapshot of the selected channel; absent when no
  // fresh queue sample exists (queue-aware routing disabled or stale).
  queue?: QueueSnapshot | null
}
type Response<T> = { success: boolean; message?: string; data: T }
export async function getPolicyConfig() {
  const response = await api.get<Response<PolicyConfig>>(
    '/api/option/request_policy'
  )
  return requireServerSuccess(response.data).data
}
export async function savePolicyConfig(options: Record<string, string>) {
  const response = await api.patch<Response<PolicyConfig>>(
    '/api/option/request_policy',
    { options }
  )
  return requireServerSuccess(response.data).data
}
