/** Exact LTE family admitted for the bounded, on-demand relay trial. */
export function isT815xLteCamera(device: { readonly model: string; readonly deviceType: number | null }): boolean {
  return device.deviceType === 110 && /^T815[0-3]$/.test(device.model);
}

/** Budget covers existing 20s lookup, 20s first-frame wait and JPEG extraction. */
export const T815X_SNAPSHOT_TIMEOUT_MILLISECONDS = 45_000;
