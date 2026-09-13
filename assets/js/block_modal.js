// -----------------------------------------------------------------------------
// Role: Owns Topic Broadcast modal behavior hooks.
// File Name: block_modal.js
// Author: Alexandre EL
// Email: alex@hackinvent.com
// Created Date: 2026-08-25
// -----------------------------------------------------------------------------

(() => {
  /**
   * Disable the incompatible Required choice on every Topic Broadcast input.
   *
   * @param {HTMLElement} root - Mounted modal root containing generic port rows.
   */
  function lockRequiredInputChoices(root) {
    root.querySelectorAll('.port-requirement-input[value="required_for_execution"]').forEach((input) => {
      input.disabled = true;
      input.closest("label")?.setAttribute("title", "Les entrées Topic Broadcast restent toujours optionnelles.");
    });
  }

  /**
   * Mark the Topic Broadcast modal as mounted while generic bindings own edits.
   *
   * @param {CustomEvent} event - Block modal mount event from the shared UI shell.
   */
  function markTopicBroadcastModalReady(event) {
    const root = event?.detail?.root;
    if (!(root instanceof HTMLElement) || root.dataset.nodeKind !== "topic_broadcast") {
      return;
    }
    root.dataset.topicBroadcastModalReady = "true";
    lockRequiredInputChoices(root);
  }

  document.addEventListener("cw:block-modal-mounted", markTopicBroadcastModalReady);
})();
