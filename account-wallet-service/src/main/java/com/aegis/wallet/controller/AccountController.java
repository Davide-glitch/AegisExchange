package com.aegis.wallet.controller;

import com.aegis.wallet.entity.ShareHolding;
import com.aegis.wallet.entity.TraderAccount;
import com.aegis.wallet.repo.ShareHoldingRepository;
import com.aegis.wallet.repo.TraderAccountRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final TraderAccountRepository accountRepo;
    private final ShareHoldingRepository holdingRepo;

    @PostMapping
    public ResponseEntity<TraderAccount> createOrTopUpAccount(
            @RequestParam String userId,
            @RequestParam(defaultValue = "100000.00") BigDecimal initialCash) {
        TraderAccount account = accountRepo.findById(userId)
                .orElse(new TraderAccount(userId, initialCash, BigDecimal.ZERO));
        return ResponseEntity.ok(accountRepo.save(account));
    }

    @GetMapping("/me")
    public ResponseEntity<Map<String, Object>> getMyAccount(@RequestParam(defaultValue = "trader-1") String userId) {
        TraderAccount account = accountRepo.findById(userId).orElseThrow();
        List<ShareHolding> holdings = holdingRepo.findByUserId(userId);
        return ResponseEntity.ok(Map.of(
                "account", account,
                "holdings", holdings
        ));
    }
}
