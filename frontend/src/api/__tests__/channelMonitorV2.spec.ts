import { afterEach, describe, expect, it, vi } from 'vitest'
import { apiClient } from '../client'
import { getMatrix, getUserRank, repeatedArrayParamsSerializer } from '../channelMonitorV2'

afterEach(() => vi.restoreAllMocks())

describe('channel monitor V2 query serialization', () => {
  it('uses repeated keys without bracket suffixes for array filters', () => {
    const query = repeatedArrayParamsSerializer({
      range: '120m',
      platform: ['openai', 'grok'],
      group_id: [1, 2],
      model: undefined,
      group_by: 'platform_group_model',
    })

    expect(query).toBe('range=120m&platform=openai&platform=grok&group_id=1&group_id=2&group_by=platform_group_model')
    expect(query).not.toContain('%5B%5D')
  })

  it('sends the matrix grouping with the shared filters', async () => {
    const get = vi.spyOn(apiClient, 'get').mockResolvedValue({
      data: { coverage: {}, group_by: 'platform_group', items: [] },
    })

    await getMatrix({ range: '24h', platforms: ['openai'], groupIds: [7], models: [] }, 'platform_group', true)

    expect(get).toHaveBeenCalledWith('/admin/channel-monitor-v2/matrix', expect.objectContaining({
      params: {
        range: '24h',
        platform: ['openai'],
        group_id: [7],
        model: undefined,
        group_by: 'platform_group',
      },
    }))
  })
})

it('looks up a user only through the admin endpoint with the current filters', async () => {
  const controller = new AbortController()
  const get = vi.spyOn(apiClient, 'get').mockResolvedValue({ data: { items: [{ user_id: 25, rank: 25 }] } })
  const result = await getUserRank({ range: '7d', platforms: ['openai'], groupIds: [7], models: [] }, 'alice', controller.signal)
  expect(result?.rank).toBe(25)
  expect(get).toHaveBeenCalledWith('/admin/channel-monitor-v2/users', expect.objectContaining({
    signal: controller.signal,
    params: expect.objectContaining({ range: '7d', username: 'alice', group_id: [7] }),
  }))
})
