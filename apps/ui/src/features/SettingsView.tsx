import { useMemo } from "hono/jsx/dom";
import { Button, Card, Input, MultiSelect, Switch } from "../components/ui";
import type { RuntimeProxyConfig, SettingsForm } from "../core/types";

type SettingsViewProps = {
	settingsForm: SettingsForm;
	adminPasswordSet: boolean;
	isSaving: boolean;
	runtimeConfig?: RuntimeProxyConfig | null;
	retryErrorCodeOptions: string[];
	onSubmit: (event: Event) => void;
	onFormChange: (patch: Partial<SettingsForm>) => void;
};

const streamUsageModes = [
	{ value: "full", label: "完整", hint: "全量解析" },
	{ value: "lite", label: "轻量", hint: "降低开销" },
	{ value: "off", label: "关闭", hint: "仅记录基础" },
] as const;

/**
 * Renders the settings view.
 *
 * Args:
 *   props: Settings view props.
 *
 * Returns:
 *   Settings JSX element.
 */
export const SettingsView = ({
	settingsForm,
	adminPasswordSet,
	isSaving,
	runtimeConfig,
	retryErrorCodeOptions,
	onSubmit,
	onFormChange,
}: SettingsViewProps) => {
	const attemptWorkerBoundValue =
		runtimeConfig === null || runtimeConfig === undefined
			? "-"
			: runtimeConfig.attempt_worker_bound
				? "已绑定"
				: "未绑定";
	const attemptWorkerActiveValue =
		runtimeConfig === null || runtimeConfig === undefined
			? "-"
			: runtimeConfig.attempt_worker_fallback_active
				? "是"
				: "否";
	const mergedRetryErrorCodeOptions = useMemo(() => {
		const all = new Set<string>();
		for (const code of retryErrorCodeOptions) {
			const normalized = String(code ?? "").trim();
			if (normalized) {
				all.add(normalized);
			}
		}
		for (const code of settingsForm.proxy_retry_skip_error_codes) {
			const normalized = String(code ?? "").trim();
			if (normalized) {
				all.add(normalized);
			}
		}
		for (const code of settingsForm.proxy_retry_sleep_error_codes) {
			const normalized = String(code ?? "").trim();
			if (normalized) {
				all.add(normalized);
			}
		}
		return Array.from(all)
			.sort((left, right) => left.localeCompare(right))
			.map((code) => ({ value: code, label: code }));
	}, [
		retryErrorCodeOptions,
		settingsForm.proxy_retry_skip_error_codes,
		settingsForm.proxy_retry_sleep_error_codes,
	]);

	return (
		<div class="animate-fade-up space-y-4">
			<div class="flex items-center justify-between">
				<div>
					<h3 class="app-title text-lg">系统设置</h3>
					<p class="app-subtitle">管理全部运行参数</p>
				</div>
			</div>

			<form class="app-settings-panel" onSubmit={onSubmit}>
				<Card class="app-settings-group">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">基础运行</h4>
						<p class="app-settings-group__caption">会话与调度策略</p>
					</div>
					<div class="app-settings-list">
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label class="app-settings-row__label" for="retention">
									日志保留天数
								</label>
								<p class="app-settings-row__hint">按天自动清理历史记录</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="retention"
								name="log_retention_days"
								type="number"
								min="1"
								value={settingsForm.log_retention_days}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({ log_retention_days: target?.value ?? "" });
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label class="app-settings-row__label" for="session-ttl">
									会话时长（小时）
								</label>
								<p class="app-settings-row__hint">管理员登录有效时长</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="session-ttl"
								name="session_ttl_hours"
								type="number"
								min="1"
								value={settingsForm.session_ttl_hours}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({ session_ttl_hours: target?.value ?? "" });
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="checkin-schedule-time"
								>
									签到时间（中国时间）
								</label>
								<p class="app-settings-row__hint">每天自动签到任务执行时间</p>
							</div>
							<Input
								class="app-settings-row__control"
								id="checkin-schedule-time"
								name="checkin_schedule_time"
								type="time"
								value={settingsForm.checkin_schedule_time}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({ checkin_schedule_time: target?.value ?? "" });
								}}
							/>
						</div>

						<div class="app-settings-row app-settings-row--stack">
							<div class="app-settings-row__main">
								<label class="app-settings-row__label" for="admin-password">
									管理员密码
								</label>
								<p class="app-settings-row__hint">
									{adminPasswordSet
										? "已设置，留空则不修改"
										: "未设置，保存后即为登录密码"}
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--full"
								id="admin-password"
								name="admin_password"
								type="password"
								placeholder={
									adminPasswordSet ? "输入新密码以覆盖" : "输入管理员密码"
								}
								value={settingsForm.admin_password}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({ admin_password: target?.value ?? "" });
								}}
							/>
						</div>
					</div>
				</Card>

				<Card class="app-settings-group app-settings-group--allow-overflow">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">代理请求</h4>
						<p class="app-settings-group__caption">上游调用与重试策略</p>
					</div>
					<div class="app-settings-list app-settings-list--allow-overflow">
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-upstream-timeout"
								>
									上游超时（毫秒）
								</label>
								<p class="app-settings-row__hint">设置为 0 表示不限制超时</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-upstream-timeout"
								name="proxy_upstream_timeout_ms"
								type="number"
								min="0"
								value={settingsForm.proxy_upstream_timeout_ms}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_upstream_timeout_ms: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label class="app-settings-row__label" for="proxy-retry-max">
									重发次数
								</label>
								<p class="app-settings-row__hint">
									0 表示不重发，默认 3 次（跨渠道重发）
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-retry-max"
								name="proxy_retry_max_retries"
								type="number"
								min="0"
								step="1"
								value={settingsForm.proxy_retry_max_retries}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_retry_max_retries: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-retry-sleep-ms"
								>
									等待时间（毫秒）
								</label>
								<p class="app-settings-row__hint">错误后二次请求等待时间</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-retry-sleep-ms"
								name="proxy_retry_sleep_ms"
								type="number"
								min="0"
								step="1"
								value={settingsForm.proxy_retry_sleep_ms}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_retry_sleep_ms: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row app-settings-row--stack">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">
									需要跳过重试的错误码
								</span>
								<p class="app-settings-row__hint">错误后无需重试的列表</p>
							</div>
							<MultiSelect
								class="app-settings-row__control app-settings-row__control--full"
								options={mergedRetryErrorCodeOptions}
								value={settingsForm.proxy_retry_skip_error_codes}
								placeholder="选择需要跳过的错误码"
								searchPlaceholder="搜索错误码"
								emptyLabel="暂无可选错误码"
								onChange={(next) => {
									onFormChange({
										proxy_retry_skip_error_codes: next,
									});
								}}
							/>
						</div>
						<div class="app-settings-row app-settings-row--stack">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">
									需要等待后重试的错误码
								</span>
								<p class="app-settings-row__hint">错误后重试需等待的列表</p>
							</div>
							<MultiSelect
								class="app-settings-row__control app-settings-row__control--full"
								options={mergedRetryErrorCodeOptions}
								value={settingsForm.proxy_retry_sleep_error_codes}
								placeholder="选择需要等待的错误码"
								searchPlaceholder="搜索错误码"
								emptyLabel="暂无可选错误码"
								onChange={(next) => {
									onFormChange({
										proxy_retry_sleep_error_codes: next,
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">无输出 视为失败</span>
								<p class="app-settings-row__hint">
									输出 Tokens 为 0 的结果会触发重试
								</p>
							</div>
							<div class="app-settings-row__switch">
								<Switch
									checked={settingsForm.proxy_zero_completion_as_error_enabled}
									onToggle={(next) => {
										onFormChange({
											proxy_zero_completion_as_error_enabled: next,
										});
									}}
								/>
							</div>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-model-failure-cooldown"
								>
									失败冷却时长（分钟）
								</label>
								<p class="app-settings-row__hint">
									同一模型连续失败达到阈值后，在该时长内跳过对应渠道；0
									表示关闭冷却
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-model-failure-cooldown"
								name="proxy_model_failure_cooldown_minutes"
								type="number"
								min="0"
								value={settingsForm.proxy_model_failure_cooldown_minutes}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_model_failure_cooldown_minutes: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-model-failure-threshold"
								>
									连续失败次数阈值
								</label>
								<p class="app-settings-row__hint">
									达到该次数才进入冷却，最小为 1
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-model-failure-threshold"
								name="proxy_model_failure_cooldown_threshold"
								type="number"
								min="1"
								step="1"
								value={settingsForm.proxy_model_failure_cooldown_threshold}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_model_failure_cooldown_threshold: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-responses-affinity-ttl"
								>
									会话粘滞缓存时长（秒）
								</label>
								<p class="app-settings-row__hint">
									用于连续会话请求锁定同一渠道，最小 60 秒
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-responses-affinity-ttl"
								name="proxy_responses_affinity_ttl_seconds"
								type="number"
								min="60"
								step="1"
								value={settingsForm.proxy_responses_affinity_ttl_seconds}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_responses_affinity_ttl_seconds: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-stream-options-capability-ttl"
								>
									参数兼容缓存时长（秒）
								</label>
								<p class="app-settings-row__hint">
									用于缓存渠道对 stream_options 参数的兼容性，最小 60 秒
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-stream-options-capability-ttl"
								name="proxy_stream_options_capability_ttl_seconds"
								type="number"
								min="60"
								step="1"
								value={settingsForm.proxy_stream_options_capability_ttl_seconds}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_stream_options_capability_ttl_seconds:
											target?.value ?? "",
									});
								}}
							/>
						</div>
					</div>
				</Card>

				<Card class="app-settings-group app-settings-group--allow-overflow">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">调用执行器</h4>
						<p class="app-settings-group__caption">单次调用执行与异常回退</p>
					</div>
					<div class="app-settings-list app-settings-list--allow-overflow">
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">
									启用调用执行器异常回退
								</span>
								<p class="app-settings-row__hint">
									当调用执行器出现异常时，按阈值切换为本地直连，提升请求稳定性
								</p>
							</div>
							<div class="app-settings-row__switch">
								<Switch
									checked={settingsForm.proxy_attempt_worker_fallback_enabled}
									onToggle={(next) => {
										onFormChange({
											proxy_attempt_worker_fallback_enabled: next,
										});
									}}
								/>
							</div>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-attempt-worker-fallback-threshold"
								>
									异常阈值（次/请求）
								</label>
								<p class="app-settings-row__hint">
									单个请求内达到该异常次数后，后续执行会自动切为本地直连，最小 1
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-attempt-worker-fallback-threshold"
								name="proxy_attempt_worker_fallback_threshold"
								type="number"
								min="1"
								step="1"
								disabled={!settingsForm.proxy_attempt_worker_fallback_enabled}
								value={settingsForm.proxy_attempt_worker_fallback_threshold}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_attempt_worker_fallback_threshold:
											target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-large-request-offload-threshold"
								>
									大请求下沉阈值（字节）
								</label>
								<p class="app-settings-row__hint">
									达到该体积后才触发下沉；0 表示所有请求都下沉
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-large-request-offload-threshold"
								name="proxy_large_request_offload_threshold_bytes"
								type="number"
								min="0"
								step="1"
								value={settingsForm.proxy_large_request_offload_threshold_bytes}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_large_request_offload_threshold_bytes:
											target?.value ?? "",
									});
								}}
							/>
						</div>
					</div>
					<div class="app-settings-stats">
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">调用执行器绑定</div>
							<div class="app-settings-stat__value">
								{attemptWorkerBoundValue}
							</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">回退策略生效</div>
							<div class="app-settings-stat__value">
								{attemptWorkerActiveValue}
							</div>
						</div>
					</div>
				</Card>

				<Card class="app-settings-group">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">解析</h4>
						<p class="app-settings-group__caption">流式解析参数</p>
					</div>
					<div class="app-settings-list">
						<div class="app-settings-row app-settings-row--stack">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">流式 usage 解析模式</span>
								<p class="app-settings-row__hint">
									选择完整解析、轻量解析或关闭解析
								</p>
							</div>
							<div
								class="app-segment app-settings-row__control app-settings-row__control--full"
								role="radiogroup"
								aria-label="流式 usage 解析模式"
							>
								{streamUsageModes.map((mode) => {
									const active =
										settingsForm.proxy_stream_usage_mode === mode.value;
									return (
										<button
											aria-pressed={active}
											class={`app-segment__button ${
												active ? "app-segment__button--active" : ""
											}`}
											key={mode.value}
											type="button"
											onClick={() =>
												onFormChange({
													proxy_stream_usage_mode: mode.value,
												})
											}
										>
											<span>{mode.label}</span>
											<small>{mode.hint}</small>
										</button>
									);
								})}
							</div>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-stream-usage-max-bytes"
								>
									流式解析最大字节
								</label>
								<p class="app-settings-row__hint">0 表示不限制</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-stream-usage-max-bytes"
								name="proxy_stream_usage_max_bytes"
								type="number"
								min="0"
								value={settingsForm.proxy_stream_usage_max_bytes}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_stream_usage_max_bytes: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-stream-usage-max-parsers"
								>
									流式解析并发上限
								</label>
								<p class="app-settings-row__hint">0 表示不限制</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-stream-usage-max-parsers"
								name="proxy_stream_usage_max_parsers"
								type="number"
								min="0"
								value={settingsForm.proxy_stream_usage_max_parsers}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_stream_usage_max_parsers: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-stream-usage-parse-timeout"
								>
									流式解析超时（毫秒）
								</label>
								<p class="app-settings-row__hint">
									SSE usage 解析任务的超时时间，0 表示不限制
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-stream-usage-parse-timeout"
								name="proxy_stream_usage_parse_timeout_ms"
								type="number"
								min="0"
								value={settingsForm.proxy_stream_usage_parse_timeout_ms}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_stream_usage_parse_timeout_ms: target?.value ?? "",
									});
								}}
							/>
						</div>
					</div>
				</Card>

				<div class="app-settings-footer">
					<Button variant="primary" size="lg" type="submit" disabled={isSaving}>
						{isSaving ? "保存中..." : "保存设置"}
					</Button>
				</div>
			</form>
		</div>
	);
};
