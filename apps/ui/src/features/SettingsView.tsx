import { Button, Card, Input, Switch } from "../components/ui";
import type {
	RuntimeProxyConfig,
	SettingsForm,
	UsageQueueStatus,
} from "../core/types";

type SettingsViewProps = {
	settingsForm: SettingsForm;
	adminPasswordSet: boolean;
	isSaving: boolean;
	runtimeConfig?: RuntimeProxyConfig | null;
	usageQueueStatus?: UsageQueueStatus | null;
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
	usageQueueStatus,
	onSubmit,
	onFormChange,
}: SettingsViewProps) => {
	const queueBoundValue =
		runtimeConfig === null || runtimeConfig === undefined
			? "-"
			: runtimeConfig.usage_queue_bound
				? "已绑定"
				: "未绑定";
	const queueActiveValue =
		runtimeConfig === null || runtimeConfig === undefined
			? "-"
			: runtimeConfig.usage_queue_active
				? "是"
				: "否";
	const formatRatio = (value: number | null | undefined): string => {
		if (typeof value !== "number" || Number.isNaN(value)) {
			return "-";
		}
		return `${(value * 100).toFixed(1)}%`;
	};
	const queueReservedValue = usageQueueStatus
		? usageQueueStatus.count === null
			? "未绑定"
			: usageQueueStatus.limit > 0
				? `${usageQueueStatus.count} / ${usageQueueStatus.limit}`
				: String(usageQueueStatus.count)
		: "-";
	const queueEnqueueSuccessValue = usageQueueStatus
		? usageQueueStatus.enqueue_success_count === null
			? "未绑定"
			: String(usageQueueStatus.enqueue_success_count)
		: "-";
	const queueDirectValue = usageQueueStatus
		? usageQueueStatus.direct_count === null
			? "未绑定"
			: String(usageQueueStatus.direct_count)
		: "-";
	const queueFallbackDirectValue = usageQueueStatus
		? usageQueueStatus.fallback_direct_count === null
			? "未绑定"
			: String(usageQueueStatus.fallback_direct_count)
		: "-";
	const queueRatioValue = usageQueueStatus
		? `${formatRatio(usageQueueStatus.effective_queue_ratio)} / ${formatRatio(
				usageQueueStatus.target_queue_ratio,
			)}`
		: "-";
	const directRatioValue = usageQueueStatus
		? `${formatRatio(usageQueueStatus.effective_direct_ratio)} / ${formatRatio(
				usageQueueStatus.target_direct_ratio,
			)}`
		: "-";

	return (
		<div class="animate-fade-up space-y-4">
			<div class="flex items-center justify-between">
				<div>
					<h3 class="app-title text-lg">系统设置</h3>
					<p class="app-subtitle">管理全部运行参数。</p>
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
								<p class="app-settings-row__hint">按天自动清理历史记录。</p>
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
								<p class="app-settings-row__hint">管理员登录有效时长。</p>
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
								<p class="app-settings-row__hint">每天自动签到任务执行时间。</p>
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
										? "已设置，留空则不修改。"
										: "未设置，保存后即为登录密码。"}
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

				<Card class="app-settings-group">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">代理请求</h4>
						<p class="app-settings-group__caption">上游调用与重试策略</p>
					</div>
					<div class="app-settings-list">
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-upstream-timeout"
								>
									上游超时（毫秒）
								</label>
								<p class="app-settings-row__hint">设置为 0 表示不限制超时。</p>
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
									0 表示不重发，默认 3 次（跨渠道重发）。
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
									for="proxy-model-failure-cooldown"
								>
									失败冷却时长（分钟）
								</label>
								<p class="app-settings-row__hint">
									同一模型连续失败达到阈值后，在该时长内跳过对应渠道；0
									表示关闭冷却。
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
									达到该次数才进入冷却，最小为 1。
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
									for="proxy-usage-breaker"
								>
									预占熔断时长（毫秒）
								</label>
								<p class="app-settings-row__hint">
									预占失败后在该时长内跳过预占。
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-usage-breaker"
								name="proxy_usage_reserve_breaker_ms"
								type="number"
								min="0"
								value={settingsForm.proxy_usage_reserve_breaker_ms}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_usage_reserve_breaker_ms: target?.value ?? "",
									});
								}}
							/>
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
									选择完整解析、轻量解析或关闭解析。
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
								<p class="app-settings-row__hint">0 表示不限制。</p>
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
								<p class="app-settings-row__hint">0 表示不限制。</p>
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
									SSE usage 解析任务的超时时间。
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
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="proxy-usage-error-max-length"
								>
									错误消息最大长度
								</label>
								<p class="app-settings-row__hint">
									usage 错误信息写入日志前的截断长度。
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="proxy-usage-error-max-length"
								name="proxy_usage_error_message_max_length"
								type="number"
								min="1"
								value={settingsForm.proxy_usage_error_message_max_length}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										proxy_usage_error_message_max_length: target?.value ?? "",
									});
								}}
							/>
						</div>
					</div>
				</Card>

				<Card class="app-settings-group">
					<div class="app-settings-group__header">
						<h4 class="app-settings-group__title">用量队列</h4>
						<p class="app-settings-group__caption">日志写入分流与状态</p>
					</div>
					<div class="app-settings-list">
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<span class="app-settings-row__label">启用用量队列</span>
								<p class="app-settings-row__hint">
									启用后将按比例把 usage 写入队列。
								</p>
							</div>
							<div class="app-settings-row__switch">
								<Switch
									checked={settingsForm.proxy_usage_queue_enabled}
									onToggle={(next) => {
										onFormChange({ proxy_usage_queue_enabled: next });
									}}
								/>
							</div>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="usage-queue-daily-limit"
								>
									队列日限额
								</label>
								<p class="app-settings-row__hint">
									达到上限后自动切回 Worker 直写。
								</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="usage-queue-daily-limit"
								name="usage_queue_daily_limit"
								type="number"
								min="0"
								value={settingsForm.usage_queue_daily_limit}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										usage_queue_daily_limit: target?.value ?? "",
									});
								}}
							/>
						</div>
						<div class="app-settings-row">
							<div class="app-settings-row__main">
								<label
									class="app-settings-row__label"
									for="usage-queue-direct-ratio"
								>
									直写比例（0-1）
								</label>
								<p class="app-settings-row__hint">示例：0.5 表示 50% 直写。</p>
							</div>
							<Input
								class="app-settings-row__control app-settings-row__control--compact"
								id="usage-queue-direct-ratio"
								name="usage_queue_direct_write_ratio"
								type="number"
								min="0"
								max="1"
								step="0.01"
								value={settingsForm.usage_queue_direct_write_ratio}
								onInput={(event) => {
									const target = event.currentTarget as HTMLInputElement | null;
									onFormChange({
										usage_queue_direct_write_ratio: target?.value ?? "",
									});
								}}
							/>
						</div>
					</div>
					<div class="app-settings-stats">
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">队列绑定</div>
							<div class="app-settings-stat__value">{queueBoundValue}</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">队列实际生效</div>
							<div class="app-settings-stat__value">{queueActiveValue}</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">队列预占数量</div>
							<div class="app-settings-stat__value">{queueReservedValue}</div>
							<div class="app-settings-stat__hint">
								{usageQueueStatus?.date
									? `统计日期：${usageQueueStatus.date}`
									: "统计日期：-"}
							</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">真实入队数量</div>
							<div class="app-settings-stat__value">
								{queueEnqueueSuccessValue}
							</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">真实直写数量</div>
							<div class="app-settings-stat__value">{queueDirectValue}</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">回退直写数量</div>
							<div class="app-settings-stat__value">
								{queueFallbackDirectValue}
							</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">队列比例（实际/目标）</div>
							<div class="app-settings-stat__value">{queueRatioValue}</div>
						</div>
						<div class="app-settings-stat">
							<div class="app-settings-stat__label">直写比例（实际/目标）</div>
							<div class="app-settings-stat__value">{directRatioValue}</div>
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
