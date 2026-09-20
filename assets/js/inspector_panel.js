// -----------------------------------------------------------------------------
// Role: Owns Topic Broadcast inspector surface mounting.
// File Name: inspector_panel.js
// Author: Alexandre EL
// Email: alex@hackinvent.com
// Created Date: 2026-08-25
// -----------------------------------------------------------------------------
/**
 * Disable the incompatible Required choice on every Topic Broadcast input.
 *
 * @param {HTMLElement} root - Mounted inspector containing generic port rows.
 */
function lockRequiredInputChoices(root) {
  root.querySelectorAll('.port-requirement-input[value="required_for_execution"]').forEach((input) => {
    input.disabled = true;
    input.closest("label")?.setAttribute("title", "Les entrées Topic Broadcast restent toujours optionnelles.");
  });
}

/**
 * Mark the mounted inspector; generic field and port bindings own mutations.
 *
 * @param {HTMLElement} root - Mounted Topic Broadcast inspector root.
 */
export function mount(root) {
  root.dataset.topicBroadcastInspectorReady = "true";
  lockRequiredInputChoices(root);
}
