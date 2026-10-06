// ============================================================
// NEXORA FOREX — Even/Odd Recovery Strategy (bot "LITECOIN")
// ============================================================
// RECONSTRUÇÃO a partir dos comentários de EvenOddRecoveryParams
// (bot.types.ts) e do padrão do scalper.strategy.ts. Se encontrares o
// original, ele tem prioridade sobre este ficheiro.
//
// Contrato: DIGITEVEN ou DIGITODD (conforme targetParity), duração
// vinda do config (normalmente 1 tick). Orientada a eventos de tick.
//
// Entrada: só quando houve `streakTrigger` ticks seguidos com a paridade
// OPOSTA à escolhida. Isto NÃO muda a probabilidade do próximo dígito;
// apenas decide QUANDO entrar (streakTrigger = 0 → entra em todos os ticks).
//
// Stake após perda (calculado sobre o payout real da Deriv, via proposal):
//   1.ª perda seguida  → stake de RECUPERAÇÃO: recupera a perda acumulada
//                        do ciclo + recoveryMarginPercent %
//   2.ª perda em diante → stake PROTETOR: protectiveStakePercent % do stake
//                        inicial (protege a banca em vez de escalar)
//   perdas seguidas >= maxMartingaleSteps → corta: reseta o ciclo
//   Em qualquer caso: nunca abaixo de minStake, NUNCA acima de maxStakeCap.
//   Vitória → reseta o ciclo e volta ao stake inicial.
//
// STOP LOSS / TAKE PROFIT: config.maxLoss / config.maxProfit (BaseStrategy).
// AVISO: sem vantagem estatística; recuperação aumenta o risco. Só em conta demo.
// ============================================================

import { BotConfig, EvenOddRecoveryParams, TradeResult, DerivWsAdapter } from '../bot.types.js';
import { BaseStrategy } from './base.strategy.js';
import { decimalsFromPip, exitDigitOf, lastDigit, withDefaults } from './digit-utils.js';

const FALLBACK_PAYOUT_RATIO = 0.95;     // lucro por unidade de stake se a proposal falhar
const PAYOUT_TTL_MS = 60_000;
const DEFAULT_MIN_STAKE = 0.35;         // mínimo habitual da Deriv

export class EvenOddRecoveryStrategy extends BaseStrategy {
  private params: EvenOddRecoveryParams;
  private pending = false;
  private paused = false;
  private lossCount = 0;       // perdas seguidas
  private cycleLoss = 0;       // perda acumulada do ciclo atual (para o stake de recuperação)
  private lossAccum = 0;       // perda acumulada para a pausa automática
  private oppositeStreak = 0;  // ticks seguidos com a paridade oposta
  private pipDecimals?: number;
  private payoutCache: { symbol: string; ratio: number; at: number } | null = null;
  private tickSub: { unsubscribe: () => void } | null = null;
  private pauseTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(id: string, name: string, config: BotConfig, derivWs: DerivWsAdapter) {
    const params = withDefaults<EvenOddRecoveryParams>({
      targetParity: 'even',
      streakTrigger: 3,
      recoveryMarginPercent: 10,
      protectiveStakePercent: 100,
      maxMartingaleSteps: 5,
      maxStakeCap: config.initialStake * 10,   // teto por omissão: 10× o stake inicial
      minStake: DEFAULT_MIN_STAKE,
      pauseLossLimit: 0,
      pauseSeconds: 10,
      decimalPlaces: 2,
    }, config.strategyParams);

    super(id, name, 'even_odd_recovery', config, derivWs);
    this.params = params;
    this.skipTickLoop = true;
  }

  protected async onStart(): Promise<void> {
    this.state.stats.currentStake = this.state.config.initialStake;
    this.resetCycle();
    this.lossAccum = 0;
    this.paused = false;
    this.pending = false;
    this.oppositeStreak = 0;
    this.payoutCache = null;

    this.log(
      'info',
      `Even/Odd Recovery — symbol: ${this.state.config.symbol}, alvo: ${this.params.targetParity}, ` +
      `gatilho: ${this.params.streakTrigger} ticks opostos, stake inicial: ${this.money(this.state.config.initialStake)}, ` +
      `margem de recuperação: ${this.params.recoveryMarginPercent}%`,
    );

    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  protected async onStop(): Promise<void> {
    this.pending = false;
    this.tickSub?.unsubscribe();
    this.tickSub = null;
    if (this.pauseTimer) {
      clearTimeout(this.pauseTimer);
      this.pauseTimer = null;
    }
  }

  protected async tick(): Promise<void> { /* no-op: ação em onTick() */ }

  // Mudança de mercado: re-subscrever ticks e reaprender casas decimais/payout
  protected async onSymbolChanged(): Promise<void> {
    this.pipDecimals = undefined;
    this.payoutCache = null;
    this.oppositeStreak = 0;
    this.tickSub?.unsubscribe();
    this.tickSub = this.derivWs.subscribeToTicks(
      this.state.config.symbol,
      (price: number, pipSize?: number) => this.onTick(price, pipSize),
    );
  }

  // ─── Lógica orientada a tick ──────────────────────────────

  private get decimals(): number {
    return this.pipDecimals ?? this.params.decimalPlaces ?? 2;
  }

  private get contractType(): 'DIGITEVEN' | 'DIGITODD' {
    return this.params.targetParity === 'odd' ? 'DIGITODD' : 'DIGITEVEN';
  }

  private onTick(price: number, pipSize?: number): void {
    if (pipSize !== undefined && this.pipDecimals === undefined) {
      this.pipDecimals = decimalsFromPip(pipSize, this.params.decimalPlaces ?? 2);
    }
    // com um trade em curso (ou pausa) não contamos a sequência: depois de
    // cada trade espera-se uma sequência NOVA
    if (!this.active || this.pending || this.paused || this.coolingDown) return;

    const digit = lastDigit(price, this.decimals);
    const isTargetParity = (digit % 2 === 0) === (this.params.targetParity !== 'odd');
    this.oppositeStreak = isTargetParity ? 0 : this.oppositeStreak + 1;

    if (this.oppositeStreak >= this.params.streakTrigger) {
      this.oppositeStreak = 0;
      void this.placeTrade();
    }
  }

  private async placeTrade(): Promise<void> {
    this.pending = true;
    const { symbol, duration, durationUnit, currency } = this.state.config;
    const contractType = this.contractType;
    const stake = this.state.stats.currentStake;

    this.log('trade', `[Even/Odd] Abrindo ${contractType} — stake: ${this.money(stake.toFixed(2))}`, {
      stake, lossCount: this.lossCount, cycleLoss: this.cycleLoss,
    });

    try {
      const entryTime = new Date();
      const { contractId } = await this.derivWs.buyContract({
        symbol,
        contractType,
        stake,
        duration,
        durationUnit,
        currency,
      });

      this.emit('bot:trade_opened', {
        type: 'bot:trade_opened',
        botId: this.state.id,
        payload: { contractId, stake, contractType },
      });

      const result = await this.derivWs.waitForContract(contractId);
      const tradeResult: TradeResult = {
        contractId,
        profit: result.profit,
        stake,
        won: result.won,
        entryTick: result.entryTick,
        exitTick: result.exitTick,
        exitDigit: exitDigitOf(result, this.decimals),
        entryTime,
        exitTime: new Date(),
      };

      this.tradeContractType = contractType;
      this.recordTradeResult(tradeResult);
      await this.adjustStake(result.won, Math.abs(result.profit));

      const nextStake = this.state.stats.currentStake.toFixed(2);
      this.log('trade', result.won
        ? `✅ Ganhou ${this.money(result.profit.toFixed(2))} — próximo stake: ${this.money(nextStake)}`
        : `❌ Perdeu ${this.money(Math.abs(result.profit).toFixed(2))} — próximo stake: ${this.money(nextStake)}`);

      this.checkStopConditions();
      if (!this.active) return;
      this.checkPauseCondition();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('error', `[Even/Odd] Falha ao operar: ${message}`, { stake });
      await this.handleUnexpectedError(err);
    } finally {
      this.pending = false;
      this.oppositeStreak = 0;
    }
  }

  // ─── Stake de recuperação / protetor ──────────────────────

  private resetCycle(): void {
    this.lossCount = 0;
    this.cycleLoss = 0;
  }

  /** Lucro por unidade de stake (payout/stake − 1), com base numa proposal real da Deriv. */
  private async payoutRatio(): Promise<number> {
    const { symbol, duration, durationUnit, currency, initialStake } = this.state.config;
    const cached = this.payoutCache;
    if (cached && cached.symbol === symbol && Date.now() - cached.at < PAYOUT_TTL_MS) {
      return cached.ratio;
    }

    try {
      const { payout } = await this.derivWs.getProposal({
        symbol,
        contractType: this.contractType,
        duration,
        durationUnit,
        currency,
        stake: initialStake,
      });
      const ratio = payout / initialStake - 1;
      if (Number.isFinite(ratio) && ratio > 0.01) {
        this.payoutCache = { symbol, ratio, at: Date.now() };
        return ratio;
      }
      this.log('warn', `[Even/Odd] Payout inválido na proposal (${payout}) — a usar ${FALLBACK_PAYOUT_RATIO}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      this.log('warn', `[Even/Odd] Proposal falhou (${message}) — a usar payout de ${FALLBACK_PAYOUT_RATIO}`);
    }
    return FALLBACK_PAYOUT_RATIO;
  }

  private clampStake(value: number): number {
    const { minStake, maxStakeCap } = this.params;
    let stake = Math.max(value, minStake ?? DEFAULT_MIN_STAKE);
    if (maxStakeCap > 0) stake = Math.min(stake, maxStakeCap); // teto absoluto: NUNCA ultrapassado
    return this.round(stake);
  }

  private async adjustStake(won: boolean, lossValue: number): Promise<void> {
    const { recoveryMarginPercent, protectiveStakePercent, maxMartingaleSteps } = this.params;
    const initial = this.state.config.initialStake;

    if (won) {
      this.resetCycle();
      this.lossAccum = 0;
      this.state.stats.currentStake = initial;
      return;
    }

    this.lossCount += 1;
    this.cycleLoss += lossValue;
    this.lossAccum += lossValue;

    // teto de perdas seguidas → corta e reseta o ciclo
    if (maxMartingaleSteps > 0 && this.lossCount >= maxMartingaleSteps) {
      this.log('info', `[Even/Odd] ${this.lossCount} perdas seguidas — corte de perdas, ciclo reiniciado.`);
      this.resetCycle();
      this.state.stats.currentStake = initial;
      return;
    }

    if (this.lossCount === 1) {
      // recuperação: perda do ciclo + margem, sobre o payout real
      const ratio = await this.payoutRatio();
      const target = this.cycleLoss * (1 + recoveryMarginPercent / 100);
      const needed = Math.ceil((target / ratio) * 100) / 100;
      this.state.stats.currentStake = this.clampStake(needed);
    } else {
      // modo protetor: % do stake inicial, sem escalar
      this.state.stats.currentStake = this.clampStake(initial * (protectiveStakePercent / 100));
    }
  }

  // ─── Pausa automática temporária ──────────────────────────

  private checkPauseCondition(): void {
    const { pauseLossLimit, pauseSeconds } = this.params;
    if (pauseLossLimit <= 0 || this.lossAccum < pauseLossLimit) return;

    this.paused = true;
    this.lossAccum = 0;
    this.log('info', `Pausa automática — perda acumulada atingiu ${this.money(pauseLossLimit)}. Retomando em ${pauseSeconds}s.`);

    if (this.pauseTimer) clearTimeout(this.pauseTimer);
    this.pauseTimer = setTimeout(() => {
      this.paused = false;
      this.pauseTimer = null;
      this.oppositeStreak = 0;
      this.log('info', 'Pausa automática terminada — retomando operações.');
    }, pauseSeconds * 1000);
  }
}
