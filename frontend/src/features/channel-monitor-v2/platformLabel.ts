import { GROUP_PLATFORM_OPTIONS } from '@/constants/platforms'

// Presentation only: retain raw platform IDs in filters, requests and row keys.
export function platformLabel(value: string): string {
  return GROUP_PLATFORM_OPTIONS.find(({ value: platform }) => platform === value)?.label || value
}
