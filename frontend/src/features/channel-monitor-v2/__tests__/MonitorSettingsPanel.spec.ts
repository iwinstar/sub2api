import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import MonitorSettingsPanel from '../MonitorSettingsPanel.vue'

const mocks = vi.hoisted(() => ({
  getConfig: vi.fn(), updateConfig: vi.fn(), groups: vi.fn(),
  showError: vi.fn(), showSuccess: vi.fn(),
}))
vi.mock('vue-i18n', async (original) => ({
  ...await original<typeof import('vue-i18n')>(),
  useI18n: () => ({ t: (key: string) => key, te: () => true }),
}))
vi.mock('@/stores/app', () => ({ useAppStore: () => ({
  cachedPublicSettings: { channel_monitor_enabled: true },
  showError: mocks.showError, showSuccess: mocks.showSuccess,
}) }))
vi.mock('@/utils/featureFlags', () => ({ getChannelMonitorMode: () => 'v2', isChannelMonitorV2Mode: () => true }))
vi.mock('@/api/admin', () => ({ adminAPI: { groups: { getAllIncludingInactive: mocks.groups } } }))
vi.mock('@/api/channelMonitorV2', async (original) => ({
  ...await original<typeof import('@/api/channelMonitorV2')>(),
  getConfig: mocks.getConfig, updateConfig: mocks.updateConfig,
}))

beforeEach(() => {
  vi.clearAllMocks()
  mocks.getConfig.mockResolvedValue({ version: 1, enabled: true, refresh_interval_seconds: 300,
    retention_period: '30d', platforms: [], group_ids: [], ignored_error_categories: [],
  })
  mocks.groups.mockResolvedValue([
    { id: 1, name: 'Standard', platform: 'openai' },
    { id: 2, name: 'Discount', platform: 'openai' },
  ])
  // HTTP responses are plain JSON, not the reactive objects passed to the API.
  mocks.updateConfig.mockImplementation(async (config) => JSON.parse(JSON.stringify({ ...config, version: config.version + 1 })))
})
const setup = async () => {
  const wrapper = mount(MonitorSettingsPanel, { global: { stubs: { Icon: true, Toggle: true, RouterLink: true } } })
  await flushPromises()
  return wrapper
}
const button = (wrapper: ReturnType<typeof mount>, suffix: string) => wrapper.findAll('button').find(b => b.text() === `channelMonitorV2.settings.${suffix}`)!

describe('monitor display groups', () => {
  it('saves members and prevents assigning them twice, in the requested card position', async () => {
    const wrapper = await setup()
    const headings = wrapper.findAll('h3').map(h => h.text())
    expect(headings.indexOf('channelMonitorV2.settings.displayGroupsTitle')).toBe(headings.indexOf('channelMonitorV2.settings.groupsTitle') + 1)
    expect(headings.indexOf('channelMonitorV2.settings.errorsTitle')).toBe(headings.indexOf('channelMonitorV2.settings.displayGroupsTitle') + 1)
    await button(wrapper, 'displayGroupsAdd').trigger('click')
    await wrapper.get('input[aria-label="channelMonitorV2.settings.displayGroupsName"]').setValue('GPT channel')
    await wrapper.get('input[type="checkbox"][value="1"]').setValue(true)
    await wrapper.get('input[type="checkbox"][value="2"]').setValue(true)
    await button(wrapper, 'save').trigger('click')
    await flushPromises()
    expect(mocks.updateConfig).toHaveBeenCalledWith(expect.objectContaining({ display_groups: [expect.objectContaining({ name: 'GPT channel', group_ids: [1, 2] })] }))
    await button(wrapper, 'displayGroupsAdd').trigger('click')
    const firstGroupBoxes = wrapper.findAll('input[type="checkbox"][value="1"]')
    expect(firstGroupBoxes[0].attributes('disabled')).toBeUndefined()
    expect(firstGroupBoxes[1].attributes('disabled')).toBeDefined()
    wrapper.unmount()
  })

  it.each([400, 500, 0])('retains edits after a save failure with status %s', async (status) => {
    const wrapper = await setup()
    await button(wrapper, 'displayGroupsAdd').trigger('click')
    await button(wrapper, 'save').trigger('click')
    expect(mocks.updateConfig).not.toHaveBeenCalled()
    expect(mocks.showError).toHaveBeenCalled()
    await wrapper.get('input[aria-label="channelMonitorV2.settings.displayGroupsName"]').setValue('GPT channel')
    await wrapper.get('input[type="checkbox"][value="1"]').setValue(true)
    mocks.updateConfig.mockRejectedValueOnce({ status, message: 'save failed' })
    await button(wrapper, 'save').trigger('click')
    await flushPromises()
    expect((wrapper.get('input[aria-label="channelMonitorV2.settings.displayGroupsName"]').element as HTMLInputElement).value).toBe('GPT channel')
    expect(mocks.getConfig).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })
  it('reloads the full configuration on 409 and saves subsequent edits with the new version', async () => {
    const wrapper = await setup()
    await button(wrapper, 'displayGroupsAdd').trigger('click')
    await wrapper.get('input[aria-label="channelMonitorV2.settings.displayGroupsName"]').setValue('Old draft')
    await wrapper.get('input[type="checkbox"][value="1"]').setValue(true)
    mocks.getConfig.mockResolvedValueOnce({
      version: 8, enabled: true, refresh_interval_seconds: 60,
      retention_period: '30d', platforms: [], group_ids: [], ignored_error_categories: [],
      display_groups: [{ id: 'newer', name: 'Another admin', group_ids: [2] }],
    })
    mocks.updateConfig.mockRejectedValueOnce({ status: 409, message: 'Configuration changed' })
    await button(wrapper, 'save').trigger('click')
    await flushPromises()
    expect(mocks.getConfig).toHaveBeenCalledTimes(2)
    const name = wrapper.get('input[aria-label="channelMonitorV2.settings.displayGroupsName"]')
    expect((name.element as HTMLInputElement).value).toBe('Another admin')
    expect(button(wrapper, 'save').attributes('disabled')).toBeDefined()
    await name.setValue('Updated name')
    await button(wrapper, 'save').trigger('click')
    await flushPromises()
    expect(mocks.updateConfig).toHaveBeenLastCalledWith(expect.objectContaining({
      version: 8, refresh_interval_seconds: 60,
      display_groups: [{ id: 'newer', name: 'Updated name', group_ids: [2] }],
    }))
    expect(mocks.showSuccess).toHaveBeenCalled()
    wrapper.unmount()
  })

})
