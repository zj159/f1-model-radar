document.addEventListener("click", async (event) => {
  const button = event.target.closest("[data-copy-target]");
  if (!button) return;

  const target = document.querySelector(button.dataset.copyTarget);
  if (!target) return;

  const original = button.textContent;
  try {
    await navigator.clipboard.writeText(target.textContent.trim());
    button.textContent = "已复制";
  } catch {
    button.textContent = "复制失败";
  }

  window.setTimeout(() => {
    button.textContent = original;
  }, 1400);
});
