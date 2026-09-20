// -----------------------------------------------------------------------------
// Role: Owns Topic Broadcast modal behavior hooks.
// File Name: block_modal.js
// Author: Alexandre EL
// Email: alex@hackinvent.com
// Created Date: 2026-08-25
// -----------------------------------------------------------------------------

/**
 * Disable the incompatible Required choice on every Topic Broadcast input.
 *
 * @param {HTMLElement} root - Mounted modal root containing generic port rows.
 */
function lockRequiredInputChoices(root) {
  root.querySelectorAll('.port-requirement-input[value="required_for_execution"]').forEach((input) => {
    input.disabled = true;
    input.closest("label")?.setAttribute("title", "Topic Broadcast inputs always stay optional.");
  });
}

/**
 * Mark the Topic Broadcast modal as mounted while generic bindings own edits.
 *
 * @param {HTMLElement} root - Mounted modal root provided by the release loader.
 */
export function mount(root) {
  if (!(root instanceof HTMLElement)) {
    return;
  }
  root.dataset.topicBroadcastModalReady = "true";
  lockRequiredInputChoices(root);
}
